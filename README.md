# InterLink HTCondor sidecar
This repo contains the code of an InterLink HTCondor sidecar, i.e. a container manager plugin which interacts with an [InterLink](https://github.com/interTwin-eu/interLink/tree/main) instance and allows the deployment of pod's singularity containers on a local or remote HTCondor batch system.

## Quick start
Let's see how to deploy a full Virtual Kubelet + InterLink API + HTCondor sidecar system which allows to run Kubernetes pod's in HTCondor jobs.
First of all, let's download this repo:
```
git clone https://github.com/ttedeschi/InterLink_HTCondor_sidecar.git
```
modify the [config file](InterLinkConfig.yaml) accordingly.
Then to run the server you just have to enter:
```
cd InterLink_HTCondor_sidecar
python3 handles.py --condor-config <path_to_condor_config_file> --schedd-host <schedd_host_url> --collector-host <collector_host_url> --auth-method <authentication_method> --debug <debug_option> --proxy <path_to_proxyfile> -- --port <server_port>
```
It will be served by default at `http://0.0.0.0:8000/`. In case of GSI authentication, certificates should be placed in `/etc/grid-security/certificates`.

Once the server is running, one now needs to retrieve and compile the interLink code:
```
cd ..
git clone https://github.com/interTwin-eu/interLink.git
cd interLink
make vk
make interlink
```
and configure it accordingly:
```
export VKTOKENFILE=<path to a file containing your token fot OAuth2 proxy authentication>
export INTERLINKCONFIGPATH=$PWD/../InterLink_HTCondor_sidecar/InterLinkConfig.yaml
export KUBECONFIG=<path_to_kubeconfig>
export NODENAME=<name_of_k8s_node>
export SIDECARPORT=8000
export SIDECARSERVICE=htcondor
```
Finally, run the two executables in two different shells:
```
./bin/interlink
```
and
```
./bin/vk
```

You can test its functionality by just making http requests via command line, e.g.:
```
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
  annotations:
    slurm-job.knoc.io/flags: "--job-name=testvkub  -t 2800  --ntasks=8 --nodes=1 --mem-per-cpu=2000"
spec:
  restartPolicy: OnFailure
  containers:
  - image: /cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el8:x86_64
    volumeMounts:
    - name: foo
      mountPath: "/etc/foo"
      readOnly: true
    command:
      - sleep
      - infinity
    imagePullPolicy: Always
    name: busyecho
  dnsPolicy: ClusterFirst
  nodeSelector:
    kubernetes.io/role: agent
    beta.kubernetes.io/os: linux
    type: virtual-kubelet
    kubernetes.io/hostname: test-vk
  tolerations:
  - key: virtual-node.interlink/no-schedule
    operator: Exists
  volumes:
  - name: foo
    configMap:
      name: my-configmap
  - name: foo2
    secret:
      secretName: mysecret
```

If the image is in the form `host:<sitename>`, the plugin will submit a specific script that deploys an HTCondor worker node that joins the INFN Analysis Facility cluster, with <sitename> as argument:
```
wget --no-check-certificate https://cmsdoc.cern.ch/~spiga/condor-10.1.0-1-x86_64_CentOS7-stripped.tgz .
tar -zxvf condor-10.1.0-1-x86_64_CentOS7-stripped.tgz
cd condor-10.1.0-1-x86_64_CentOS7-stripped/
./setupwn.sh $1
sleep 3000
```
this behaviour will soon change in favour of a more general one
