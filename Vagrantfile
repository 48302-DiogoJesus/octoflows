# -*- mode: ruby -*-
# vi: set ft=ruby :

# Francisco Silva - <dpss-helpdesk@inesc-id.pt>

Vagrant.configure("2") do |config|
  # You can search for boxes at https://vagrantcloud.com/search.
  config.vm.box = "generic/ubuntu2204"

  config.ssh.insert_key = false 
  config.ssh.username = "vagrant"
  config.ssh.password = "vagrant"

  config.vm.network "forwarded_port", guest: 5000, host: 5000
  # config.vm.network "forwarded_port", guest: 6379, host: 6379
  # config.vm.network "forwarded_port", guest: 6380, host: 6380

  config.vm.provider "virtualbox" do |vb|
    vb.name = "ubuntu-24.04-vm"
    vb.gui = false
    vb.memory = "30720"
    vb.cpus = 24
    vb.customize ["modifyvm", :id, "--nested-hw-virt", "on"]
    vb.customize ["modifyvm", :id, "--nictype1", "virtio"]
  end
  
  config.vm.synced_folder "./", "/octoflows"

  # Root commands
  config.vm.provision "shell", inline: <<-SHELL
    apt-get update
    apt-get install -y software-properties-common # Required to install specific Python version
    add-apt-repository ppa:deadsnakes/ppa -y # Required to install specific Python version
    apt-get update

    # Install Python, Redis CLI and other debug tools
    apt-get install -y curl unzip git graphviz redis-tools
    apt-get install -y python3.12 python3.12-venv python3.12-dev

    # Install Docker
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    usermod -aG docker vagrant
    systemctl daemon-reload
    systemctl restart docker

    # ensure clock is synchronized to avoid drifts between diff. machines
    timedatectl set-ntp true
    
    apt-get clean
  SHELL

  # User commands
  config.vm.provision "shell", privileged: false, inline: <<-SHELL
    # Install pip3.12 and all to path
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc # Ensure path is set for the user when it logs in later
    source ~/.bashrc  
    export PATH="$HOME/.local/bin:$PATH" # Ensure path is set for current shell
    pip3.12 install -r /octoflows/src/requirements.txt
  SHELL
end