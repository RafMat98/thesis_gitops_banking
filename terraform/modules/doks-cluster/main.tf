terraform {
    required_providers {
            digitalocean = {
                source = "digitalocean/digitalocean"
                version = "~> 2.0"
        }
    }
}

data "digitalocean_vpc" "existing_vpc" {
  name = "default-fra1"
}

resource "digitalocean_kubernetes_cluster" "this" {
    name = var.cluster_name
    region = "fra1"
    version = "latest"
    vpc_uuid = data.digitalocean_vpc.existing_vpc.id
    ha = false
    node_pool {
        name = "worker-pool"
        size = var.node_size
        node_count = var.enable_autoscaling ? null : var.node_count

        auto_scale = var.enable_autoscaling
        min_nodes = var.enable_autoscaling ? var.min_nodes : null
        max_nodes = var.enable_autoscaling ? var.max_nodes : null
    }
}