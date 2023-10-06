curl -X POST -H "Content-Type: application/json" -d '{
    "Pods": [
        {
            "Pod": {
                "ObjectMeta": {
                    "Name": "my-pod-1",
                    "Namespace": "my-namespace"
                },
                "Spec": {
                    "Containers": [
                        {
                            "Name": "host-1",
                            "Image": "host:T2_LNL_PD",
                            "Command": ["echo", "Hello, HTCondor!"],
                            "Args": []
                        }
                    ]
                }
            }
        }
    ]
}' http://localhost:8000/submit
