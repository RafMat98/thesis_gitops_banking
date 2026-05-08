# Banking Microservices Infrastructure: GitOps Bootstrap

This repository contains the Infrastructure as Code (IaC) and GitOps configurations for a highly available banking microservices architecture. The infrastructure is deployed on **Proxmox VE** using **Talos Linux** as the underlying immutable operating system for the Kubernetes cluster.

This specific section covers **Task 1.2: Manual ArgoCD Install & Bootstrap**, utilizing the "App of Apps" pattern to establish a Single Source of Truth.

## Prerequisites

Before applying the configurations in this repository, ensure you have the following:

- A running **Talos Linux Kubernetes Cluster** (1 Control Plane, 2 Worker Nodes).
- `talosctl` configured and communicating with the cluster.
- `kubectl` installed and configured with the cluster's context.
- A GitHub Personal Access Token (if the repository is private).

---

## 1. ArgoCD Controller Installation

ArgoCD is deployed directly into the cluster to act as the continuous delivery controller.

```bash
# Create the dedicated namespace for ArgoCD
kubectl create namespace argocd

# Install the stable release of ArgoCD
kubectl apply -n argocd -f [https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml](https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml)
```
