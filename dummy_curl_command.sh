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
