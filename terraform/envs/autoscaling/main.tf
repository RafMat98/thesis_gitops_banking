terraform {
    cloud {
        organization = "banking-thesis-org"
        workspaces {
            name = "banking-thesis-autoscaling"
        }
    }
    required_providers {
        digitalocean = {
            source = "digitalocean/digitalocean"
            version = "~> 2.0"
        }
    }
}

variable "do_token" {
  type      = string
  sensitive = true
}

provider "digitalocean" {
  token = var.do_token
}

module "banking_thesis_autoscaling" {
    source                          = "../../modules/doks-cluster"
    node_count                      = 3
    cluster_name                    = "banking-cluster-dev"
    node_size                       = "s-2vcpu-4gb"
    enable_autoscaling              = true
    min_nodes                       = 2
    max_nodes                       = 4
}

output "dev_endpoint" {
    value = module.banking_thesis_autoscaling.cluster_endpoint
}