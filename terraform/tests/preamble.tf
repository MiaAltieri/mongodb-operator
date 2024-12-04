resource "null_resource" "preamble" {
  provisioner "local-exec" {
    command = <<-EOT
    sudo snap install juju-wait --classic || true
    sudo sysctl -w vm.max_map_count=262144 vm.swappiness=0 net.ipv4.tcp_retries2=5
    EOT
  }

}

