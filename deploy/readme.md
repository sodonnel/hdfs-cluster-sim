ansible-playbook -i ./hosts nn.yaml --ask-pass

ansible -i ./hosts namenode -a "whoami" --ask-pass -b
