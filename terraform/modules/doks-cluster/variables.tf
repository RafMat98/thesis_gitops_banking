variable "cluster_name" {
type = string
 description = "Cluster name"
}
variable "node_size" {
type = string
 default = "s-2vcpu-4gb"
}
variable "node_count" {
type = number
 default = 2
}
variable "enable_autoscaling" {
type = bool
 default = false
}
variable "min_nodes" {
type = number
 default = 2
}
variable "max_nodes" {
type = number
 default = 4
}
variable "use_existing_vpc" {
  type    = bool
  default = false
}


