output "cluster_endpoint" {
 value = digitalocean_kubernetes_cluster.this.endpoint
}
output "kube_config_raw" {
 value = digitalocean_kubernetes_cluster.this.kube_config.0.raw_config
 sensitive = true
}
