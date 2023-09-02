mc config host add minio http://${MINIO_DOMAIN:-minio}:9000 ${MINIO_USER:-admin} ${MINIO_ROOT_PASSWORD:-password}

mc rm -r --force minio/warehouse;
mc mb minio/warehouse;