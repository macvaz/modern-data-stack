mc config host add minio http://${MINIO_DOMAIN:-minio}:9000 ${MINIO_USER:-admin} ${MINIO_ROOT_PASSWORD:-password}

# Creating key pair
mc admin user svcacct add minio admin --access-key ${AWS_ACCESS_KEY_ID} --secret-key ${AWS_SECRET_ACCESS_KEY}
# Creating bucket for the lakehouse
mc mb minio/lakehouse;