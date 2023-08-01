# InterLink HTCondor sidecar
This repo contains the code of an InterLink HTCondor sidecar, i.e. a container manager plugin which interacts with an [InterLink](https://github.com/interTwin-eu/interLink/tree/main) instance and allows the deployment of pod's singularity containers on a local or remote HTCondor batch system.

## Quick start
The deployment of this web server is straightforward.
You just need to clone this repository:
```
git clone https://github.com/ttedeschi/InterLink_HTCondor_sidecar.git
```
then you just have to start the server:
```
python3 handles.py --schedd-name <name of the schedd> --schedd-host <schedd host> --collector-host <collector host> --auth-method <authentication method> --scitokens-file <path to token file> --cafile <path to ca file>  --debug <debug level>
```
It will be served by default at `http://localhost:8000/`

You can test its functionality by just making http requests via command line, e.g.:
```
curl -X POST -H "Content-Type: application/json" -d '{
  "Pods": [
    {
      "ObjectMeta": {
        "Name": "my-pod"
      },
      "Spec": {
        "Containers": [
          {
            "Name": "my-container",
            "Image": "nginx:latest",
            "Command": ["echo", "Hello, HTCondor!"],
            "Args": []
          }
        ]
      }
    }
  ]
}' http://localhost:8000/submit
```
