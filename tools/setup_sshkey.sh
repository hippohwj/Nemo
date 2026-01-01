## set ssh key permission (for cloudlab set up)
sudo cp /root/.ssh/id_rsa $1/.ssh/
sudo cp /root/.ssh/id_rsa.pub $1/.ssh/
sudo cat /root/.ssh/authorized_keys >> $1/.ssh/authorized_keys
sudo chmod +rw $1/.ssh/id_rsa
sudo chmod +rw $1/.ssh/id_rsa.pub