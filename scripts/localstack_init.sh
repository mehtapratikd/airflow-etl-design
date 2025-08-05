apt-get update
apt-get install groff -y
pip install "awscli-local[ver1]"
awslocal s3api create-bucket --bucket data
awslocal s3api list-buckets
