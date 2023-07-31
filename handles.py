import json
import os
import time
import subprocess
import logging
import htcondor
import yaml
import shutil
from kubernetes import client, config

JID = []
prefix = "" 
schedd = htcondor.Schedd()  

#type InterLinkConfig struct {
#	VKTokenFile    string `yaml:"VKTokenFile"`
#	Interlinkurl   string `yaml:"InterlinkURL"`
#	Sidecarurl     string `yaml:"SidecarURL"`
#	Sbatchpath     string `yaml:"SbatchPath"`
#	Scancelpath    string `yaml:"ScancelPath"`
#	Interlinkport  string `yaml:"InterlinkPort"`
#	Sidecarport    string
#	Sidecarservice string `yaml:"SidecarService"`
#	Commandprefix  string `yaml:"CommandPrefix"`
#	ExportPodData  bool   `yaml:"ExportPodData"`
#	DataRootFolder string `yaml:'DataRootFolder'`
#	ServiceAccount string `yaml:"ServiceAccount"`
#	Namespace      string `yaml:"Namespace"`
#	Tsocks         bool   `yaml:"Tsocks"`
#	Tsockspath     string `yaml:"TsocksPath"`
#	Tsocksconfig   string `yaml:"TsocksConfig"`
#	Tsockslogin    string `yaml:"TsocksLoginNode"`
#	set            bool
#}

def read_yaml_file(file_path):
    with open(file_path, 'r') as file:
        try:
            data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as e:
            print("Error reading YAML file:", e)
            return None

def prepare_envs(container):
    env = ["--env"]
    env_data = []
    for env_var in container.env:
        env_data.append(f"{env_var.name}={env_var.value}")
    env.append(",".join(env_data))
    return env

def prepare_mounts(container, pod):
    mounts = ["--bind"]
    mount_data = []
    pod_name = container['Name'].split("-")[:6] if len(container['Name'].split("-")) > 6 else container['Name'].split("-")
    pod_volume_spec = None
    pod_name_folder = os.path.join(InterLinkConfigInst['DataRootFolder'], "-".join(pod_name[:-1]))

    try:
        os.makedirs(pod_name_folder, exist_ok=True)
        logging.info(f"Successfully created folder {pod_name_folder}")
    except Exception as e:
        logging.error(e)

    for mount_var in container["volume_mounts"]:
        path = ""
        for vol in pod.spec.volumes:
            if vol.name == mount_var.name:
                pod_volume_spec = vol.volume_source
            if pod_volume_spec and pod_volume_spec.config_map:
                config_maps_paths, envs = mount_config_maps(container, pod)
                for i, path in enumerate(config_maps_paths):
                    if os.getenv("SHARED_FS") != "true":
                        dirs = path.split(":")
                        split_dirs = dirs[0].split("/")
                        dir_ = os.path.join(*split_dirs[:-1])
                        prefix += f"\nmkdir -p {dir_} && touch {dirs[0]} && echo ${envs[i]} > {dirs[0]}"
                    mount_data.append(path)
            elif pod_volume_spec and pod_volume_spec.secret:
                secrets_paths, envs = mount_secrets(container, pod)
                for i, path in enumerate(secrets_paths):
                    if os.getenv("SHARED_FS") != "true":
                        dirs = path.split(":")
                        split_dirs = dirs[0].split("/")
                        dir_ = os.path.join(*split_dirs[:-1])
                        prefix += f"\nmkdir -p {dir_} && touch {dirs[0]} && echo ${envs[i]} > {dirs[0]}"
                    mount_data.append(path)
            elif pod_volume_spec and pod_volume_spec.empty_dir:
                path = mount_empty_dir(container, pod)
                mount_data.append(path)
            else:
                # Implement logic for other volume types if required.
                logging.info("\n*******************\n*To be implemented*\n*******************")

    path_hardcoded = ("/cvmfs/grid.cern.ch/etc/grid-security:/etc/grid-security" + "," +
                      "/cvmfs:/cvmfs" + "," +
                      "/exa5/scratch/user/spigad" + "," +
                      "/exa5/scratch/user/spigad/CMS/SITECONF" + ",")
    mount_data.append(path_hardcoded)
    mounts.append(",".join(mount_data))
    return mounts

def mount_config_maps(container, pod):
    config_maps = {}
    config_map_name_paths = []
    envs = []

    if InterLinkConfigInst['ExportPodData']:
        cmd = ["-rf", os.path.join(InterLinkConfigInst['DataRootFolder'], "configMaps")]
        subprocess.run(["rm"] + cmd, check=True)

        for mount_spec in container.volume_mounts:
            pod_volume_spec = None

            for vol in pod.spec.volumes:
                if vol.name == mount_spec.name:
                    pod_volume_spec = vol.volume_source
                    break

            if pod_volume_spec and pod_volume_spec.config_map:
                cmvs = pod_volume_spec.config_map
                mode = os.FileMode(*pod_volume_spec.config_map.default_mode)
                pod_config_map_dir = os.path.join(InterLinkConfigInst['DataRootFolder'],
                                                  pod.namespace + "-" + str(pod.uid) + "/configMaps/", vol.name)

                config_map = client.CoreV1Api().read_namespaced_config_map(cmvs.name, pod.namespace)

                if config_map.data:
                    for key, value in config_map.data.items():
                        config_maps[key] = value
                        path = os.path.join(pod_config_map_dir, key) + (":" + mount_spec.mount_path + "/" + key + ",")
                        config_map_name_paths.append(path)

                        if os.getenv("SHARED_FS") != "true":
                            env = str(container.name) + "_CFG_" + key
                            os.environ[env] = value
                            envs.append(env)

                if config_maps:
                    if os.getenv("SHARED_FS") == "true":
                        cmd = ["-p", pod_config_map_dir]
                        subprocess.run(["mkdir"] + cmd, check=True)

                        for key, value in config_maps.items():
                            full_path = os.path.join(pod_config_map_dir, key)
                            with open(full_path, "w") as f:
                                f.write(value)

    return config_map_name_paths, envs

def mount_secrets(container, pod):
    secrets = {}
    secret_name_paths = []
    envs = []

    if InterLinkConfigInst['ExportPodData']:
        cmd = ["-rf", os.path.join(InterLinkConfigInst['DataRootFolder'], "secrets")]
        subprocess.run(["rm"] + cmd, check=True)

        for mount_spec in container.volume_mounts:
            pod_volume_spec = None

            for vol in pod.spec.volumes:
                if vol.name == mount_spec.name:
                    pod_volume_spec = vol.volume_source
                    break

            if pod_volume_spec and pod_volume_spec.secret:
                svs = pod_volume_spec.secret
                mode = os.FileMode(*pod_volume_spec.secret.default_mode)
                pod_secret_dir = os.path.join(InterLinkConfigInst['DataRootFolder'],
                                              pod.namespace + "-" + str(pod.uid) + "/secrets/", vol.name)

                secret = client.CoreV1Api().read_namespaced_secret(svs.secret_name, pod.namespace)

                if secret.data:
                    for key, value in secret.data.items():
                        secrets[key] = value
                        path = os.path.join(pod_secret_dir, key) + (":" + mount_spec.mount_path + "/" + key + ",")
                        secret_name_paths.append(path)

                        if os.getenv("SHARED_FS") != "true":
                            env = str(container.name) + "_SECRET_" + key
                            os.environ[env] = value
                            envs.append(env)

                if secrets:
                    if os.getenv("SHARED_FS") == "true":
                        cmd = ["-p", pod_secret_dir]
                        subprocess.run(["mkdir"] + cmd, check=True)

                        for key, value in secrets.items():
                            full_path = os.path.join(pod_secret_dir, key)
                            with open(full_path, "wb") as f:
                                f.write(value)

    return secret_name_paths, envs

def mount_empty_dir(container, pod):
    ed_path = None
    if InterLinkConfigInst['ExportPodData']:
        cmd = ["-rf", os.path.join(InterLinkConfigInst['DataRootFolder'], "emptyDirs")]
        subprocess.run(["rm"] + cmd, check=True)
        for mount_spec in container.volume_mounts:
            pod_volume_spec = None
            for vol in pod.spec.volumes:
                if vol.name == mount_spec.name:
                    pod_volume_spec = vol.volume_source
                    break
            if pod_volume_spec and pod_volume_spec.empty_dir:
                ed_path = os.path.join(InterLinkConfigInst['DataRootFolder'],
                                       pod.namespace + "-" + str(pod.uid) + "/emptyDirs/" + vol.name)
                cmd = ["-p", ed_path]
                subprocess.run(["mkdir"] + cmd, check=True)
                ed_path += (":" + mount_spec.mount_path + "/" + mount_spec.name + ",")

    return ed_path

def produce_htcondor_script(container, metadata, command):
    executable_path = f"/tmp/{container['Name']}.sh"
    with open(executable_path, "w") as f:
        prefix += f"\n{InterLinkConfigInst['Commandprefix']}"
        batch_macros = f"""#!/bin/bash
        . ~/.bash_profile
        export SINGULARITYENV_SINGULARITY_TMPDIR=$CINECA_SCRATCH
        export SINGULARITYENV_SINGULARITY_CACHEDIR=$CINECA_SCRATCH
        pwd; hostname; date{prefix}; {command};
        """    
    job = {
        "executable": "{}".format(executable_path),  # the program to run on the execute node
        "output": "{}{}.out".format(InterLinkConfigInst['DataRootFolder'], container['Name']) ,      # anything the job prints to standard output will end up in this file
        "error": "{}{}.err".format(InterLinkConfigInst['DataRootFolder'], container['Name']) ,         # anything the job prints to standard error will end up in this file
        "log": "{}{}.log".format(InterLinkConfigInst['DataRootFolder'], container['Name'])   ,          # this file will contain a record of what happened to the job
        "request_cpus": "1",            # how many CPU cores we want 
        "request_memory": "128MB",      # how much memory we want
        "request_disk": "128MB",        # how much disk space we want
        } 
        
    if "htcondor-job.knoc.io/sitename" in metadata.annotations:
        sitename = metadata.annotations["htcondor-job.knoc.io/sitename"]
        job["requirements"] = f'(SiteName == "{sitename}")'

    return htcondor.Submit(job)

def htcondor_batch_submit(job):
    logging.info("Submitting HTCondor job")
    submit_result = schedd.submit(job, spool = True, ) 
    return submit_result

def delete_container(container):
    logging.info(f"Deleting container {container['Name']}")
    with open(f"{InterLinkConfigInst['DataRootFolder']}{container['Name']}.jid") as f:
        data = f.read()
    jid = int(data.strip())
    schedd.act(htcondor.JobAction.Remove, f"ClusterId == {jid}") 
    
    os.remove(f"{InterLinkConfigInst['DataRootFolder']}{container['Name']}.out")
    os.remove(f"{InterLinkConfigInst['DataRootFolder']}{container['Name']}.err")
    os.remove(f"{InterLinkConfigInst['DataRootFolder']}{container['Name']}.jid")
    os.remove(f"{InterLinkConfigInst['DataRootFolder']}{container['Name']}")

def SubmitHandler(w, r):
    logging.info("HTCondor Sidecar: received Submit call")
    body_bytes = r.read()
    try:
        req = json.loads(body_bytes)
    except json.JSONDecodeError as e:
        logging.error("Error decoding JSON:", e)
        return
    if os.getenv("KUBECONFIG") == "":
        time.sleep(1)
    try:
        config.load_kube_config(os.getenv("KUBECONFIG"))
        api_client = client.ApiClient()
        clientset = client.CoreV1Api(api_client)
    except Exception as e:
        logging.error("Unable to create a valid config:", e)
        return
    for pod in req.get("Pods", []):
        metadata = pod.get("ObjectMeta", {})
        containers = pod.get("Spec", {}).get("Containers", [])
        for container in containers:
            logging.info(f"Beginning script generation for container {container['Name']}")
            commstr1 = ["singularity", "exec"]

            envs = prepare_envs(container)
            image = ""
            mounts = prepare_mounts(container, pod)
            if container["Image"].startswith("/"):
                image_uri = metadata.get("Annotations", {}).get("htcondor-job.knoc.io/image-root", None)
                if image_uri:
                    logging.info(image_uri)
                    image = image_uri + container["Image"]
                else:
                    logging.warning("image-uri annotation not specified for path in remote filesystem")
            else:
                image = "docker://" + container["Image"]
            image = container["Image"]

            logging.info("Appending all commands together...")
            singularity_command = commstr1 + envs + mounts + [image] + container["Command"] + container["Args"]

            path = produce_htcondor_script(container, metadata, singularity_command)
            out = htcondor_batch_submit(path)
            JID.append(out.cluster(), pod)
            logging.info(out)

            try:
                with open(InterLinkConfigInst['DataRootFolder'] + container['Name'] + ".jid", "r") as f:
                    jid = f.read()
                JID.append({"JID": jid, "Pod": pod})
            except FileNotFoundError:
                logging.error("Unable to read JID from file")

def StopHandler(w, r):
    logging.info("HTCondor Sidecar: received Stop call")
    body_bytes = r.read()
    try:
        req = json.loads(body_bytes)
    except json.JSONDecodeError as e:
        logging.error("Error decoding JSON:", e)
        return
    for pod in req.get("Pods", []):
        containers = pod.get("Spec", {}).get("Containers", [])
        for container in containers:
            delete_container(container)

def StatusHandler(w, r):
    logging.info("HTCondor Sidecar: received GetStatus call")
    body_bytes = r.read()
    try:
        req = json.loads(body_bytes)
    except json.JSONDecodeError as e:
        logging.error("Error decoding JSON:", e)
        return
    resp = {"PodName": [], "PodStatus": [], "ReturnVal": "Status"}
    for pod in req.get("Pods", []):
        for jid in JID:
            resp["PodName"].append({'Name': pod.get('Name', "")})
            query_result = schedd.query(constraint=f"ClusterId == {jid}", projection=["ClusterId", "ProcId", "Out"],)   
            if len(query_result) == 1:
                resp["PodStatus"].append({"PodStatus": "RUNNING"})
            else:
                resp["PodStatus"].append({"PodStatus": "STOP"})
    w.write(json.dumps(resp))

def SetKubeCFGHandler(w, r):
    logging.info("HTCondor Sidecar: received SetKubeCFG call")
    path = "/tmp/.kube/"
    ret_code = "200"
    body_bytes = r.read()
    try:
        req = json.loads(body_bytes)
    except json.JSONDecodeError as e:
        logging.error("Error decoding JSON:", e)
        w.write(ret_code.encode())
        return
    logging.info("Creating folder to save KubeConfig")
    try:
        os.makedirs(path, exist_ok=True)
        logging.info("Successfully created folder")
    except Exception as e:
        logging.error(e)
        ret_code = "500"
        w.write(ret_code.encode())
        return
    logging.info("Creating the actual KubeConfig file")
    try:
        with open(path + "config", "w") as config_file:
            config_file.write(req.get("Body", ""))
        logging.info("Successfully created file")
    except Exception as e:
        logging.error(e)
        ret_code = "500"
        w.write(ret_code.encode())
        return
    logging.info("Setting KUBECONFIG env")
    try:
        os.environ["KUBECONFIG"] = path + "config"
        logging.info(f"Successfully set KUBECONFIG to {path}config")
    except Exception as e:
        logging.error(e)
        ret_code = "500"
        w.write(ret_code.encode())
        return
    w.write(ret_code.encode())

# The above functions can be used as handlers for appropriate endpoints in your web server.
from flask import Flask, request

interlink_config_path = "./InterLinkConfig.yaml"
global InterLinkConfigInst 
InterLinkConfigInst = read_yaml_file(interlink_config_path)
                                     
app = Flask(__name__)
app.add_url_rule('/submit', view_func=SubmitHandler, methods=['POST'])
app.add_url_rule('/stop', view_func=StopHandler, methods=['POST'])
app.add_url_rule('/status', view_func=StatusHandler, methods=['POST'])
app.add_url_rule('/set_kube_config', view_func=SetKubeCFGHandler, methods=['POST'])

if __name__ == '__main__':
    app.run(port=8000)