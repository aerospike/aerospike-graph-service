This example helm chart will deploy a basic instance of the graph service.
A helper script is included to illustrate giving helm command line arguments

To use this script, provide the pod-name to create, the Aerospike host string, and the name of the namespace to use

>----------------------------------------------------------------------------------------<
  helm-example.sh HELM_ACTION POD_NAME REPLICA_COUNT AEROSPIKE_HOST AEROSPIKE_NAMESPACE
>----------------------------------------------------------------------------------------<
$ scripts/helm-example.sh install test-pod 3 10.32.32.77:3000 test
NAME: test-pod
LAST DEPLOYED: Thu Nov 30 17:02:35 2023
NAMESPACE: default
STATUS: deployed
REVISION: 1
NOTES:
1. Get the application URL by running these commands:
  http://graph-service.aerospike.demo/gremlin


...

$ kubectl get pods
NAME                                     READY   STATUS    RESTARTS   AGE
test-pod-graphservice-697dcdfcb8-28sb4   1/1     Running   0          114s
test-pod-graphservice-697dcdfcb8-4smj6   1/1     Running   0          114s
test-pod-graphservice-697dcdfcb8-lcjwp   1/1     Running   0          114s

...

$ bash scripts/helm-example.sh upgrade test-pod 1 10.32.32.77:3000 test
...
REVISION: 2
...


$ kubectl get pods
NAME                                     READY   STATUS        RESTARTS   AGE
test-pod-graphservice-697dcdfcb8-28sb4   1/1     Running       0          5m14s
test-pod-graphservice-697dcdfcb8-4smj6   1/1     Terminating   0          5m14s
test-pod-graphservice-697dcdfcb8-lcjwp   1/1     Terminating   0          5m14s



To get the port allocated by the load balancer, you will need to check k8s services

$ kubectl get services
NAME                    TYPE           CLUSTER-IP      EXTERNAL-IP   PORT(S)          AGE
kubernetes              ClusterIP      10.96.0.1       <none>        443/TCP          163m
test-pod-graphservice   LoadBalancer   10.98.251.199   <pending>     8182:31343/TCP   6m35s

This indicates the graph service has been provisioned on the load balancer on port 31343
*Note: if you are testing using minikube, you may need to use the "minikube tunnel" command

It is likely you will need to edit helm/graphservice/values.yaml

This contains information about how to setup your load balancer, the release tag, and other important settings.
Some of this information will likely be specific to your k8s cluster configuration. 

