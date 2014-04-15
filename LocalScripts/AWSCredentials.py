aws_access_key_id='AKIAILXLBSRPY54JL24A'
aws_secret_access_key='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'
# Get the Keypair in the EC2 Dashboard page.
keyPairFile="/Users/lige/Dropbox/cs/cse291/UCSD_BigData/LocalScripts/cse291nv.pem" # name of file keeping local key
key_name="cse291nv" # name of keypair (not name of file where key is stored)
# Set the security group On the EC2 page (You will need to add IP addresses if
# you want to connect from a place previously unauthorized.
security_groups=['launch-wizard-1']
### End of AWS credentials ####
