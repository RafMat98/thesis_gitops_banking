terraform {
    cloud {
        organization = "banking-thesis-org"
        workspaces {
            name = "banking-thesis-basic"
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

module "banking_thesis_standard" {
    source                          = "../../modules/doks-cluster"
    node_count                      = 3
    cluster_name                    = "banking-cluster-dev"
    node_size                       = "s-2vcpu-4gb"
}

output "dev_endpoint" {
    value = module.banking_thesis_standard.cluster_endpoint
}