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
                            "Name": "container-1",
                            "Image": "nginx:latest",
                            "Command": ["echo", "Hello, HTCondor!"],
                            "Args": [],
                            "VolumeMounts": [
                                {
                                    "Name": "config-map-vol",
                                    "MountPath": "/etc/config"
                                },
                                {
                                    "Name": "secret-vol",
                                    "MountPath": "/etc/secret"
                                }
                            ]
                        }
                    ],
                    "Volumes": [
                        {
                            "Name": "config-map-vol",
                            "VolumeSource": {
                                "ConfigMap": {
                                    "Name": "my-config-map-1",
                                    "Data": {
                                        "key1": "value1",
                                        "key2": "value2"
                                    }
                                }
                            }
                        },
                        {
                            "Name": "secret-vol",
                            "VolumeSource": {
                                "Secret": {
                                    "SecretName": "my-secret-1",
                                    "Data": {
                                        "username": "admin",
                                        "password": "secretpassword"
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        },
        {
            "Pod": {
                "ObjectMeta": {
                    "Name": "my-pod-2",
                    "Namespace": "my-namespace"
                },
                "Spec": {
                    "Containers": [
                        {
                            "Name": "container-2",
                            "Image": "alpine:latest",
                            "Command": ["echo", "Hello, Kubernetes!"],
                            "Args": []
                        }
                    ]
                }
            }
        }
    ]
}' http://localhost:8000/submit
