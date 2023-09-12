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
parser.add_argument("--cadir", help="CA directory", type=str, default = "")
parser.add_argument("--certfile", help="cert file", type=str, default = "")
parser.add_argument("--keyfile", help="key file", type=str, default = "")
parser.add_argument("--auth-method", help="Default authentication methods", type=str, default = "")
parser.add_argument("--debug", help="Debug level", type=str, default = "")
parser.add_argument("--condor-config", help="Path to condor_config file", type=str, default = "")
parser.add_argument("--proxy", help="Path to proxy file", type=str, default = "")
parser.add_argument("--dummy-job", action = 'store_true', help="Whether the job should be a real job or a dummy sleep job for debugging purposes")
parser.add_argument("--port", help="Server port", type=int, default = 8000)

args = parser.parse_args()

if args.schedd_name != "":
    os.environ['_condor_SCHEDD_NAME'] = args.schedd_name
if args.schedd_host != "":
    os.environ['_condor_SCHEDD_HOST'] = args.schedd_host
if args.collector_host != "":
    os.environ['_condor_COLLECTOR_HOST'] = args.collector_host
if args.cadir != "":
    os.environ['_condor_AUTH_SSL_CLIENT_CADIR'] = args.cadir
if args.certfile != "":
    os.environ['_condor_AUTH_SSL_CLIENT_CERTFILE'] = args.certfile
if args.keyfile != "":
    os.environ['_condor_AUTH_SSL_CLIENT_KEYFILE'] = args.keyfile
if args.auth_method != "":
    os.environ['_condor_SEC_DEFAULT_AUTHENTICATION_METHODS'] = args.auth_method
if args.debug != "":
    os.environ['_condor_TOOL_DEBUG'] = args.debug
if args.condor_config != "":
    os.environ['CONDOR_CONFIG'] = args.condor_config
if args.proxy != "":
    os.environ['X509_USER_PROXY'] = args.proxy
if args.proxy != "":
    os.environ['X509_USER_CERT'] = args.proxy
dummy_job = args.dummy_job


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
interlink_config_path = "/utils/InterLinkConfig.yaml"
#interlink_config_path = "./InterLinkConfig.yaml"
InterLinkConfigInst = read_yaml_file(interlink_config_path)
print("Interlink configuration info:", InterLinkConfigInst)

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

def prepare_mounts(pod, container_standalone):
    mounts = ["--bind"]
    mount_data = []
    pod_name = container_standalone['name'].split("-")[:6] if len(container_standalone['name'].split("-")) > 6 else container_standalone['name'].split("-")
    pod_volume_spec = None
    pod_name_folder = os.path.join(InterLinkConfigInst['DataRootFolder'], "-".join(pod_name[:-1]))
    for c in pod['spec']['containers']:
        if c['name'] == container_standalone['name']:
            container = c
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
                    continue
                if "configMap" in vol.keys():
                    config_maps_paths = mountConfigMaps(pod, container_standalone)
                    print("bind as configmap", mount_var["name"], vol["name"])
                    for i, path in enumerate(config_maps_paths):
                        mount_data.append(path)
                elif "secret" in vol.keys():
                    secrets_paths = mountSecrets(pod, container_standalone)
                    print("bind as secret", mount_var["name"], vol["name"])
                    for i, path in enumerate(secrets_paths):
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

def mountConfigMaps(pod, container_standalone):
    configMapNamePaths = []
    wd = os.getcwd()
    for c in pod['spec']['containers']:
        if c['name'] == container_standalone['name']:
            container = c
    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        data_root_folder = InterLinkConfigInst["DataRootFolder"]
        cmd = ["-rf", os.path.join(wd, data_root_folder, "configMaps")]
        shell = subprocess.Popen(["rm"] + cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, err = shell.communicate()

        if err:
            logging.error("Unable to delete root folder")

        for mountSpec in container["volumeMounts"]:
            podVolumeSpec = None
            for vol in pod["spec"]["volumes"]:
                if vol["name"] != mountSpec["name"]:
                    continue
                if "configMap" in vol.keys():
                    cfgMaps = container_standalone['configMaps']
                    for cfgMap in cfgMaps:
                        podConfigMapDir = os.path.join(wd, data_root_folder, f"{pod['metadata']['namespace']}-{pod['metadata']['uid']}/configMaps/", vol["name"])
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
                            with open(full_path, "w") as f:
                                f.write(v)
                            os.chmod(full_path, vol["configMap"]["defaultMode"])
                            logging.debug(f"--- Written ConfigMap file {full_path}")
                        #except Exception as e:
                        #else:
                        #    logging.error(f"Could not write ConfigMap file {full_path}: {e}")
                        #    if True:
                        #        os.remove(full_path)
                        #        logging.error(f"Unable to remove file {full_path}")
                        #        #except Exception as e:
                        #    else:
                        #        logging.error(f"Unable to remove file {full_path}: {e}")
    return configMapNamePaths


def mountSecrets(pod, container_standalone):
    secret_name_paths = []
    wd = os.getcwd()
    for c in pod['spec']['containers']:
        if c['name'] == container_standalone['name']:
            container = c
    if InterLinkConfigInst["ExportPodData"] and "volumeMounts" in container.keys():
        data_root_folder = InterLinkConfigInst["DataRootFolder"]
        cmd = ["-rf", os.path.join(wd, data_root_folder, "secrets")]
        subprocess.run(["rm"] + cmd, check=True)
        for mount_spec in container["volumeMounts"]:
            pod_volume_spec = None
            for vol in pod["spec"]["volumes"]:
                if vol["name"] == mount_spec["name"]:
                    pod_volume_spec = vol["volumeSource"]
                    break
            if "Secret" in vol.keys():
                secrets = container_standalone['secrets']
                for secret in secrets:
                    pod_secret_dir = os.path.join(wd, data_root_folder, f"{pod['metadata']['namespace']}-{pod['metadata']['uid']}/secrets/", vol["name"])
                    for key in secret["data"]:
                        path = os.path.join(pod_secret_dir, key)
                        path += f":{mount_spec['MountPath']}/{key} "
                        secret_name_paths.append(path)
                    cmd = ["-p", pod_secret_dir]
                    subprocess.run(["mkdir"] + cmd, check=True)
                    logging.debug(f"--- Created folder {pod_secret_dir}")
                    logging.debug("--- Writing Secret files")
                    for k, v in secret["Data"].items():
                        full_path = os.path.join(pod_secret_dir, k)
                        with open(full_path, "w") as f:
                            f.write(v)
                        os.chmod(full_path, oct(vol["secret"]["defaultMode"]))
                        logging.debug(f"--- Written Secret file {full_path}")
                    #else:
                    #    logging.error(f"Could not write Secret file {full_path}: {e}")
                    #    try:
                    #        os.remove(full_path)
                    #        logging.error(f"Unable to remove file {full_path}")
                    #    except Exception as e:
                    #        logging.error(f"Unable to remove file {full_path}: {e}")
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
    #try:
    if True:
        with open(executable_path, "w") as f:
            batch_macros = f"""#!/bin/bash
"""
            commands_joined = []
            for i in range(0,len(commands)):
                commands_joined.append(" ".join(commands[i]))
            f.write(batch_macros + "\n" + "\n".join(commands_joined))

        job = f"""
Executable = {executable_path}

Log        = log/mm_mul.$(Cluster).$(Process).log
Output     = out/mm_mul.out.$(Cluster).$(Process)
Error      = err/mm_mul.err.$(Cluster).$(Process)

should_transfer_files = YES
RequestCpus = {requested_cpus}
RequestMemory = {requested_memory}

when_to_transfer_output = ON_EXIT
+MaxWallTimeMins = 60

+WMAgent_AgentName = "whatever"

Queue 1
"""
        print(job)
        with open(sub_path, "w") as f_:
            f_.write(job)
        os.chmod(executable_path, 0o0777)
    #except Exception as e:
    else:
        logging.error(f"Unable to prepare the job: {e}")

    return sub_path


def produce_htcondor_host_script(container, metadata):
    executable_path = f"./{InterLinkConfigInst['DataRootFolder']}/{container['name']}.sh"
    sub_path = f"./{InterLinkConfigInst['DataRootFolder']}/{container['name']}.jdl"
    if True:
        with open(executable_path, "w") as f:
            if dummy_job == False:
                prefix_ = f"\n{InterLinkConfigInst['CommandPrefix']}"
                batch_macros = f"""#!{container['command'][-1]}
""" + '\n'.join(container['args'][-1].split("; "))

            f.write(batch_macros)

        requested_cpu = container['resources']['requests']['cpu']
        #requested_memory = int(container['resources']['requests']['memory'])/1e6
        requested_memory = container['resources']['requests']['memory']
        job = f"""
Executable = {executable_path}

Log        = log/mm_mul.$(Cluster).$(Process).log
Output     = out/mm_mul.out.$(Cluster).$(Process)
Error      = err/mm_mul.err.$(Cluster).$(Process)

should_transfer_files = YES
RequestCpus = {requested_cpu}
RequestMemory = {requested_memory}

when_to_transfer_output = ON_EXIT
+MaxWallTimeMins = 60

+WMAgent_AgentName = "whatever"

Queue 1
"""
        print(job)
        with open(sub_path, "w") as f_:
            f_.write(job)
        os.chmod(executable_path, 0o0777)
    #except Exception as e:
    else:
        logging.error(f"Unable to prepare the job: {e}")

    return sub_path

def htcondor_batch_submit(job):
    logging.info("Submitting HTCondor job")
    process = os.popen(f"condor_submit -pool {args.collector_host} -remote {args.schedd_host} {job} -spool")
    preprocessed = process.read()
    process.close()
    jid = preprocessed.split(" ")[-1].split(".")[0]

    return jid


def delete_pod(pod):
    logging.info(f"Deleting pod {pod['metadata']['name']}")
    with open(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.jid") as f:
        data = f.read()
    jid = int(data.strip())
    process = os.popen(f"condor_rm {jid}")
    preprocessed = process.read()
    process.close()

    #os.remove(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.out")
    #os.remove(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.err")
    #os.remove(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.jid")
    #os.remove(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}")

    return preprocessed


def handle_jid(jid, pod):
    if True:
        with open(f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.jid", "w") as f:
            f.write(str(jid))
        JID.append({"JID": jid, "pod": pod})
        logging.info(f"Job {jid} submitted successfully", f"{InterLinkConfigInst['DataRootFolder']}{pod['metadata']['name']}.jid")
    else:
        logging.info("Job submission failed, couldn't retrieve JID")
        #return "Job submission failed, couldn't retrieve JID", 500

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
    print(pod)
    containers_standalone = req.get("container", {})
    print("REQUESTED POD IS: ", pod['metadata']['name'])
    metadata = pod.get("metadata", {})
    containers = pod.get("spec", {}).get("containers", [])
    singularity_commands = []

    #NORMAL CASE
    if not "host" in containers[0]["image"]:
        for container in containers:
            logging.info(f"Beginning script generation for container {container['name']}")
            commstr1 = ["singularity", "exec"]
            envs = prepare_envs(container)
            image = ""
            for c in containers_standalone:
                if c["name"] == container["name"]:
                    container_standalone = c
            mounts = prepare_mounts(pod, container_standalone)
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

    else:
        print("host keyword detected in the first container, ignoring other containers")
        sitename = containers[0]["image"].split(":")[-1]
        print(sitename)
        path = produce_htcondor_host_script(containers[0], metadata)

    out_jid = htcondor_batch_submit(path)
    print(out_jid)
    handle_jid(out_jid, pod)
    #logging.info(out)

    if True:
        with open(InterLinkConfigInst['DataRootFolder'] + pod['metadata']['name'] + ".jid", "r") as f:
            jid = f.read()
        #except FileNotFoundError:
        return "Job submitted successfully", 200
    else:
        logging.error("Unable to read JID from file")
        return "Something went wrong in job submission", 500

def StopHandler():
    ##### READ THE REQUEST ######
    logging.info("HTCondor Sidecar: received Stop call")
    request_data_string = request.data.decode("utf-8")
    req = json.loads(request_data_string)[0]
    print("REQUESTED TO DELETE POD IS", req)
    if req is None or not isinstance(req, dict):
        logging.error("Invalid request data")
        return "Invalid request data for stopping", 400

    #### DELETE JOB RELATED TO REQUEST
    if True:
        return_message = delete_pod(req)
        print(return_message)
        if "All" in return_message:
            return "Requested pod successfully deleted", 200
        else:
            return "Something went wrong when deleting the requested pod", 500
    else:
        return "Something went wrong when deleting the requested pod", 500

def StatusHandler():
    ####### READ THE REQUEST #####################
    logging.info("HTCondor Sidecar: received GetStatus call")
    request_data_string = request.data.decode("utf-8")
    req = json.loads(request_data_string)[0]
    if req is None or not isinstance(req, dict):
        logging.error("Invalid request data")
        return "Invalid request data for getting status", 400

    print("REQ IS", req)
    ####### ELABORATE RESPONSE #################
    resp = [{"Name": [], "Namespace": [], "Status": [], }]
    with open(InterLinkConfigInst['DataRootFolder'] + req['metadata']['name'] + ".jid", "r") as f:
        jid_job = f.read()
    if True:
        if True:
            this_resp = {}
            podname = req['metadata']['name']
            podnamespace = req['metadata']['namespace']
            print(type(podname), podname)
            resp[0]["Name"] = podname
            resp[0]["Namespace"] = podnamespace
            ok = True
            process = os.popen(f"condor_q {jid_job} --json")
            preprocessed = process.read()
            process.close()
            print(preprocessed)
            try:
                job_ = json.loads(preprocessed)
                status = job_[0]["JobStatus"]
                if status != 2 and status !=1:
                    ok = False
            except:
                ok = False
                logging.error("Something went wrong when retrieving job status")
            if ok == True:
                resp[0]["Status"] = 0
            else:
                resp[0]["Status"] = 1
        return json.dumps(resp), 200

    else:
        return "Something went wrong when retrieving pod status", 500

from flask import Flask, request

app = Flask(__name__)
app.add_url_rule('/create', view_func=SubmitHandler, methods=['POST'])
app.add_url_rule('/delete', view_func=StopHandler, methods=['POST'])
app.add_url_rule('/status', view_func=StatusHandler, methods=['GET'])

if __name__ == '__main__':
    app.run(port=args.port, host="0.0.0.0", debug=True)
