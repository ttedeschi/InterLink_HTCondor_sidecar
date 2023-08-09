import json
import os
import time
import subprocess
import logging
import yaml
import shutil
import argparse
parser = argparse.ArgumentParser()

parser.add_argument("--schedd-name", help="Schedd name", type=str, default = "")
parser.add_argument("--schedd-host", help="Schedd host", type=str, default = "")
parser.add_argument("--collector-host", help="Collector-host", type=str, default = "")
parser.add_argument("--scitokens-file", help="Scitokens file", type=str, default = "")
parser.add_argument("--cafile", help="CA file", type=str, default = "")
parser.add_argument("--auth-method", help="Default authentication methods", type=str, default = "")
parser.add_argument("--debug", help="Debug level", type=str, default = "")

args = parser.parse_args()

if args.schedd_name != "":
    os.environ['_condor_SCHEDD_NAME'] = args.schedd_name
if args.schedd_host != "":
    os.environ['_condor_SCHEDD_HOST'] = args.schedd_host
if args.collector_host != "":
    os.environ['_condor_COLLECTOR_HOST'] = args.collector_host
if args.scitokens_file != "":
    os.environ['_condor_SCITOKENS_FILE'] = args.scitokens_file
if args.cafile != "":
    os.environ['_condor_AUTH_SSL_CLIENT_CAFILE'] = args.cafile
if args.auth_method != "":
    os.environ['_condor_SEC_DEFAULT_AUTHENTICATION_METHODS'] = args.auth_method
if args.debug != "":
    os.environ['_condor_TOOL_DEBUG'] = args.debug


global JID
JID = []

global prefix
prefix = ""

def read_yaml_file(file_path):
    with open(file_path, 'r') as file:
        try:
            data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as e:
            print("Error reading YAML file:", e)
            return None
global InterLinkConfigInst
interlink_config_path = "../interLink/kustomizations/InterLinkConfig.yaml"
InterLinkConfigInst = read_yaml_file(interlink_config_path)
print(InterLinkConfigInst)


import htcondor
schedd = htcondor.Schedd()

def prepare_envs(container):
    env = ["--env"]
    env_data = []
    try:
        for env_var in container.env:
            env_data.append(f"{env_var.name}={env_var.value}")
        env.append(",".join(env_data))
        return env
    except:
        logging.info(f"Container has no env specified")
        return [""]

def prepare_mounts(container, pod, container_standalone):
    mounts = ["--bind"]
    mount_data = []
    pod_name = container['name'].split("-")[:6] if len(container['name'].split("-")) > 6 else container['name'].split("-")
    pod_volume_spec = None
    pod_name_folder = os.path.join(InterLinkConfigInst['DataRootFolder'], "-".join(pod_name[:-1]))

    try:
        os.makedirs(pod_name_folder, exist_ok=True)
        logging.info(f"Successfully created folder {pod_name_folder}")
    except Exception as e:
        logging.error(e)
    if "volumeMounts" in container.keys():
        for mount_var in container["volumeMounts"]:
            path = ""
            for vol in pod["spec"]["volumes"]:
                if vol["name"] != mount_var["name"]:
                    #    pod_volume_spec = vol["volumeSource"]
                    #else:
                    continue
                if "configMap" in vol.keys():
                    config_maps_paths = mountConfigMaps(container, pod,  vol["ConfigMap"], )
                    print("bind as configmap", mount_var["name"], vol["name"])
                    for i, path in enumerate(config_maps_paths):
                        if os.getenv(+"SHARED_FS") != "true":
                            dirs = path.split(":")
                            split_dirs = dirs[0].split("/")
                            dir_ = os.path.join(*split_dirs[:-1])
                            #prefix = f"\nmkdir -p {dir_} && touch {dirs[0]} && echo ${envs[i]} > {dirs[0]}"
                        mount_data.append(path)
                elif "secret" in vol.keys():
                    secrets_paths = mountSecrets(container, pod, vol["Secret"])
                    print("bind as secret", mount_var["name"], vol["name"])
                    for i, path in enumerate(secrets_paths):
                        if os.getenv("SHARED_FS") != "true":
                            dirs = path.split(":")
                            split_dirs = dirs[0].split("/")
                            dir_ = os.path.join(*split_dirs[:-1])
                            #prefix = f"\nmkdir -p {dir_} && touch {dirs[0]} && echo ${envs[i]} > {dirs[0]}"
                        mount_data.append(path)
                elif "emptyDir" in vol.keys():
                    path = mount_empty_dir(container, pod)
                    mount_data.append(path)
                else:
                    # Implement logic for other volume types if required.
                    logging.info("\n*******************\n*To be implemented*\n*******************")
    else:
        logging.info(f"Container has no volume mount")
        return [""]


    path_hardcoded = ("/cvmfs/grid.cern.ch/etc/grid-security:/etc/grid-security" + "," +
                      "/cvmfs:/cvmfs" + "," +
                      "/exa5/scratch/user/spigad" + "," +
                      "/exa5/scratch/user/spigad/CMS/SITECONF" + ",")
    mount_data.append(path_hardcoded)
    mounts.append(",".join(mount_data))
    return mounts

def mountConfigMaps(pod, container_pod, container_standalone):
    configMapNamePaths = []
    wd = os.getcwd()
    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        data_root_folder = InterLinkConfigInst["DataRootFolder"]
        #remove the directory where the ConfigMaps will be mounted
        cmd = ["-rf", os.path.join(wd, data_root_folder, "configMaps")]
        shell = subprocess.Popen(["rm"] + cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, err = shell.communicate()

        if err:
            logging.error("Unable to delete root folder")

        for mountSpec in container_pod["volumeMounts"]:
            podVolumeSpec = None
            for vol in pod["spec"]["volumes"]:
                if vol["name"] != mountSpec["name"]:
                    continue
                    #podVolumeSpec = vol["volumeSource"]
                if "configMap" in vol.keys():
                    podConfigMapDir = os.path.join(wd, data_root_folder, f"{pod['metadata']['namespace']}-{pod['metadata']['uid']}/configMaps/", vol["name"])
                    #podConfigMapDir = os.path.join(wd, data_root_folder, f"{pod['metadata']['Namespace']}/configMaps/", vol["name"])
                    #if container["Data"]:
                    if True:
                        for key in cfgMap["data"].keys():
                            path = os.path.join(wd, podConfigMapDir, key)
                            path += f":{mountSpec['mountPath']}/{key} "
                            configMapNamePaths.append(path)
                    cmd = ["-p", podConfigMapDir]
                    shell = subprocess.Popen(["mkdir"] + cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    execReturn, _ = shell.communicate()
                    if execReturn:
                        logging.error(err)
                    else:
                        logging.debug(f"--- Created folder {podConfigMapDir}")
                    logging.debug("--- Writing ConfigMaps files")
                    for k, v in cfgMap["data"].items():
                        full_path = os.path.join(podConfigMapDir, k)
                        if True:
                            with open(full_path, "w") as f:
                                f.write(v)
                            os.chmod(full_path, oct(vol["defaultMode"]))
                            logging.debug(f"--- Written ConfigMap file {full_path}")
                        #except Exception as e:
                        else:
                            logging.error(f"Could not write ConfigMap file {full_path}: {e}")
                            if True:
                                os.remove(full_path)
                                logging.error(f"Unable to remove file {full_path}")
                                #except Exception as e:
                            else:
                                logging.error(f"Unable to remove file {full_path}: {e}")
    return configMapNamePaths


def mountSecrets(container, pod, secret, secret_standalone):
    secret_name_paths = []
    wd = os.getcwd()

    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        data_root_folder = InterLinkConfigInst["DataRootFolder"]
        # Remove the directory where the secrets will be mounted
        cmd = ["-rf", os.path.join(wd, data_root_folder, "secrets")]
        subprocess.run(["rm"] + cmd, check=True)

        for mount_spec in container["volumeMounts"]:
            pod_volume_spec = None
            for vol in pod["spec"]["volumes"]:
                if vol["name"] == mount_spec["name"]:
                    pod_volume_spec = vol["volumeSource"]
                    break
            if "Secret" in vol.keys():
                pod_secret_dir = os.path.join(wd, data_root_folder, f"{pod['metadata']['namespace']}-{secret_standalone['metadata']['uid']}/secrets/", vol["name"])
                #pod_secret_dir = os.path.join(wd, data_root_folder, f"{pod['metadata']['Namespace']}", vol["name"])

                if secret["data"]:
                    for key in secret["data"]:
                        path = os.path.join(pod_secret_dir, key)
                        path += f":{mount_spec['MountPath']}/{key} "
                        secret_name_paths.append(path)

                cmd = ["-p", pod_secret_dir]
                subprocess.run(["mkdir"] + cmd, check=True)

                logging.debug(f"--- Created folder {pod_secret_dir}")
                logging.debug("--- Writing Secret files")
                for k, v in secret["Data"].items():
                    # TODO: Ensure that these files are deleted in failure cases
                    full_path = os.path.join(pod_secret_dir, k)
                    if True:
                        with open(full_path, "w") as f:
                            f.write(v)
                        os.chmod(full_path, oct(vol["defaultMode"]))
                        logging.debug(f"--- Written Secret file {full_path}")
                    else:
                        logging.error(f"Could not write Secret file {full_path}: {e}")
                        try:
                            os.remove(full_path)
                            logging.error(f"Unable to remove file {full_path}")
                        except Exception as e:
                            logging.error(f"Unable to remove file {full_path}: {e}")
    return secret_name_paths

def mount_empty_dir(container, pod):
    ed_path = None
    if InterLinkConfigInst['ExportPodData'] and "volumeMounts" in container.keys():
        cmd = ["-rf", os.path.join(InterLinkConfigInst['DataRootFolder'], "emptyDirs")]
        subprocess.run(["rm"] + cmd, check=True)
        for mount_spec in container["volumeMounts"]:
            pod_volume_spec = None
            for vol in pod["spec"]["volumes"]:
                if vol.name == mount_spec["name"]:
                    pod_volume_spec = vol["volumeSource"]
                    break
            if pod_volume_spec and pod_volume_spec["EmptyDir"]:
                ed_path = os.path.join(InterLinkConfigInst['DataRootFolder'],
                                       pod.namespace + "-" + str(pod.uid) + "/emptyDirs/" + vol.name)
                cmd = ["-p", ed_path]
                subprocess.run(["mkdir"] + cmd, check=True)
                ed_path += (":" + mount_spec["mount_path"] + "/" + mount_spec["name"] + ",")

    return ed_path

def produce_htcondor_singularity_script(containers, metadata, commands):
    executable_path = f"./{metadata['name']}.sh"
    if True:
        with open(executable_path, "w") as f:
            #prefix += f"\n{InterLinkConfigInst['CommandPrefix']}"
            prefix_ = f"\n{InterLinkConfigInst['CommandPrefix']}"
            batch_macros = f"""#!/bin/bash
sleep 100000000
. ~/.bash_profile
export SINGULARITYENV_SINGULARITY_TMPDIR=$CINECA_SCRATCH
export SINGULARITYENV_SINGULARITY_CACHEDIR=$CINECA_SCRATCH
pwd; hostname;
"""
            # date{prefix_};
            #f.write(batch_macros + "\n" + " ".join(command))
            commands_joined = []
            for i in range(0,len(commands)):
                commands_joined.append(" ".join(commands[i]))
            f.write(batch_macros + "\n" + "\n".join(commands_joined))

        job = {
            "executable": "{}".format(executable_path),  # the program to run on the execute node
            "output": "{}{}.out".format(InterLinkConfigInst['DataRootFolder'], metadata['name']) ,      # anything the job prints to standard output will end up in this file
            "error": "{}{}.err".format(InterLinkConfigInst['DataRootFolder'], metadata['name']) ,         # anything the job prints to standard error will end up in this file
            "log": "{}{}.log".format(InterLinkConfigInst['DataRootFolder'], metadata['name'])   ,          # this file will contain a record of what happened to the job
            "request_cpus": "1",            # how many CPU cores we want
            "request_memory": "128MB",      # how much memory we want
            "request_disk": "128MB",        # how much disk space we want
            }

        os.chmod(executable_path, 0o0777)
    else:
        print(InterLinkConfigInst)

    return htcondor.Submit(job)


def produce_htcondor_host_script(container, metadata, t2):
    executable_path = f"./{container['name']}.sh"
    if True:
        with open(executable_path, "w") as f:
            #prefix += f"\n{InterLinkConfigInst['CommandPrefix']}"
            prefix_ = f"\n{InterLinkConfigInst['CommandPrefix']}"
            batch_macros = f"""#!/bin/bash
sleep 100000000
echo "SiteName =" $1

SiteName=$1

echo "iniziamo ora " `date`
wget --no-check-certificate https://cmsdoc.cern.ch/~spiga/condor-10.1.0-1-x86_64_CentOS7-stripped.tgz .
tar -zxvf condor-10.1.0-1-x86_64_CentOS7-stripped.tgz
cd condor-10.1.0-1-x86_64_CentOS7-stripped/
echo " "
echo "lancio il wn "  `date`
./setupwn.sh $SiteName

sleep 14000
echo " ho aspettato 300 " `date`
ps -auxf
echo "========"
echo "========"
cat var/log/condor/MasterLog
echo "========"
echo "========"
echo "esco "
cat var/log/condor/StartLog
"""
            # date{prefix_};
            f.write(batch_macros)

        job = {
            "executable": "{}".format(executable_path),  # the program to run on the execute node
            "arguments": "{}".format(t2),  # the program to run on the execute node
            "output": "{}{}.out".format(InterLinkConfigInst['DataRootFolder'], container['name']) ,      # anything the job prints to standard output will end up in this file
            "error": "{}{}.err".format(InterLinkConfigInst['DataRootFolder'], container['name']) ,         # anything the job prints to standard error will end up in this file
            "log": "{}{}.log".format(InterLinkConfigInst['DataRootFolder'], container['name'])   ,          # this file will contain a record of what happened to the job
            "request_cpus": "1",            # how many CPU cores we want
            #"request_cpus": "8",            # how many CPU cores we want
            "request_memory": "128MB",      # how much memory we want
            #"request_memory": "16000",      # how much memory we want
            "request_disk": "128MB",        # how much disk space we want
            "when_to_transfer_output": "ON_EXIT",
            "+MaxWallTimeMins" : "60",
            "+WMAgent_AgentName": "whatever",
            #"Queue": "1"
            }

        os.chmod(executable_path, 0o0777)

    return htcondor.Submit(job)

def htcondor_batch_submit(job):
    logging.info("Submitting HTCondor job")
    submit_result = schedd.submit(job, )#spool = True, )
    return submit_result

def delete_container(container):
    logging.info(f"Deleting container {container['name']}")
    with open(f"{InterLinkConfigInst['DataRootFolder']}{container['name']}.jid") as f:
        data = f.read()
    jid = int(data.strip())
    schedd.act(htcondor.JobAction.Remove, f"ClusterId == {jid}")
    os.remove(f"{InterLinkConfigInst['DataRootFolder']}{container['name']}.jid")

def delete_pod(pod):
    logging.info(f"Deleting pod {pod['metadata']['name']}")
    with open(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.jid") as f:
        data = f.read()
    jid = int(data.strip())
    schedd.act(htcondor.JobAction.Remove, f"ClusterId == {jid}")

    #os.remove(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.out")
    #os.remove(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.err")
    os.remove(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.jid")
    #os.remove(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}")

def handle_jid(container, jid, pod):
    try:
        with open(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.jid", "w") as f:
            f.write(str(jid))
        JID.append({"JID": jid, "pod": pod})
        logging.info(f"Job {jid} submitted successfully", f"{InterLinkConfigInst['DataRootFolder']}{container['name']}.jid")
    except:
        logging.info("Job submission failed, couldn't retrieve JID")
        return "Job submission failed, couldn't retrieve JID", 500

def SubmitHandler():
    ##### READ THE REQUEST ###############
    logging.info("HTCondor Sidecar: received Submit call")
    request_data_string = request.data.decode("utf-8")
    print("Decoded", request_data_string)
    req = json.loads(request_data_string)[0]
    if req is None or not isinstance(req, dict):
        logging.error("Invalid request data for submitting")
        print("REQUEST BODY ISSSSS: ", req)
        return "Invalid request data for submitting", 400

    ###### ELABORATE RESPONSE ###########
    pod = req.get("pod", {})
    print("REQUESTED POD IS: ", pod['metadata']['name'])
    metadata = pod.get("metadata", {})
    containers = pod.get("spec", {}).get("containers", [])
    singularity_commands = []

    #NORMAL CASE
    if not "host:" in containers[0]["image"]:
        for container in containers:
            logging.info(f"Beginning script generation for container {container['name']}")
            commstr1 = ["singularity", "exec"]
            envs = prepare_envs(container)
            image = ""
            mounts = [""]
            if container["image"].startswith("/"):
                image_uri = metadata.get("Annotations", {}).get("htcondor-job.knoc.io/image-root", None)
                if image_uri:
                    logging.info(image_uri)
                    image = image_uri + container["image"]
                else:
                    logging.warning("image-uri annotation not specified for path in remote filesystem")
            else:
                image = "docker://" + container["image"]
            image = container["image"]
            logging.info("Appending all commands together...")
            if "command" in container.keys() and "args" in container.keys():
                singularity_command = commstr1 + envs + mounts + [image] + container["command"] + container["args"]
            elif "command" in container.keys():
                singularity_command = commstr1 + envs + mounts + [image] + container["command"]
            else:
                singularity_command = commstr1 + envs + mounts + [image]
            print("singularity_command:", singularity_command)
            singularity_commands.append(singularity_command)
        path = produce_htcondor_singularity_script(containers, metadata, singularity_commands)

    ### WLCG T2 HTCONDOR CASE
    else:
        print("host keyword detected in the first container, ignoring other containers")
        sitename = container["image"].split(":")[-1]
        print(sitename)
        path = produce_htcondor_host_script(container, metadata, sitename)

    out = htcondor_batch_submit(path)
    handle_jid(container, out.cluster(), pod)
    logging.info(out)

    #try:
    if True:
        with open(InterLinkConfigInst['DataRootFolder'] + pod['metadata']['name'] + ".jid", "r") as f:
            jid = f.read()
        #JID.append({"JID": jid, "Pod": pod})
        #except FileNotFoundError:
    else:
        logging.error("Unable to read JID from file")
    return "Job submitted successfully", 200

def StopHandler():
    ##### READ THE REQUEST ######
    logging.info("HTCondor Sidecar: received Stop call")
    request_data_string = request.data.decode("utf-8")
    req = json.loads(request_data_string)[0]
    if req is None or not isinstance(req, dict):
        logging.error("Invalid request data")
        return "Invalid request data for stopping", 400

    #### DELETE JOBS RELATED TO REQUEST
    pod = req.get("pod", {})
    delete_pod(pod)
    #containers = pod.get("spec", {}).get("containers", [])
    #for container in containers:
    #    delete_container(container)
    return "Requested pod successfully deleted", 200

def StatusHandler():
    ####### READ THE REQUEST #####################
    logging.info("HTCondor Sidecar: received GetStatus call")
    request_data_string = request.data.decode("utf-8")
    req = json.loads(request_data_string)[0]
    if req is None or not isinstance(req, dict):
        logging.error("Invalid request data")
        return "Invalid request data for getting status", 400

    ####### ELABORATE RESPONSE #################
    resp = {"PodName": [], "PodStatus": [], "ReturnVal": "Status"}
    for jid in JID:
        podname = jid['pod']['metadata']['name']
        print(type(podname), podname)
        resp["PodName"].append({"Name": podname})
        ok = True
        query_result = schedd.query(constraint=f"ClusterId == {jid['JID']}", projection=["ClusterId", "ProcId", "Out", "JobStatus"],)
        if len(query_result) == 0:
            ok = False
        elif query_result[0]['JobStatus'] != 2:
            ok = False
        if ok == True:
            resp["PodStatus"].append({"PodStatus": 0})
        else:
            resp["PodStatus"].append({"PodStatus": 1})
    return json.dumps(resp), 200


# The above functions can be used as handlers for appropriate endpoints in your web server.
from flask import Flask, request

app = Flask(__name__)
app.add_url_rule('/submit', view_func=SubmitHandler, methods=['POST'])
app.add_url_rule('/stop', view_func=StopHandler, methods=['POST'])
app.add_url_rule('/status', view_func=StatusHandler, methods=['GET'])

if __name__ == '__main__':
    app.run(port=8000, debug=True)
