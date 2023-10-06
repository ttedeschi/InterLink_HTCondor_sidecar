# InterLink HTCondor sidecar
This repo contains the code of an InterLink HTCondor sidecar, i.e. a container manager plugin which interacts with an [InterLink](https://github.com/interTwin-eu/interLink/tree/main) instance and allows the deployment of pod's singularity containers on a local or remote HTCondor batch system.

## Quick start
First of all, let's download this repo:
```
git clone https://github.com/ttedeschi/InterLink_HTCondor_sidecar.git
```
modify the [config file](InterLinkConfig.yaml) accordingly.
Then to run the server you just have to enter:
```
cd InterLink_HTCondor_sidecar
python3 handles.py --condor-config <path_to_condor_config_file> --schedd-host <schedd_host_url> --collector-host <collector_host_url> --auth-method <authentication_method> --debug <debug_option> --proxy <path_to_proxyfile> --port <server_port>
```
It will be served by default at `http://0.0.0.0:8000/`. In case of GSI authentication, certificates should be placed in `/etc/grid-security/certificates`.

If Virtual Kubelet and Interlink instances are running and properly configured, you can then test deploying:
```
kubectl apply -f ./tests/test_configmap.yaml
kubectl apply -f ./tests/test_secret.yaml
kubectl apply -f ./tests/busyecho_k8s.yaml
```
A special behaviour is triggered if the image is in the form `host`. The plugin will submit the script which is passed as argument:
```
kubectl apply -f ./tests/production_deployment_LNL.yaml
```
