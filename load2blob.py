from azure.storage.blob import BlobServiceClient,ContainerClient
import uuid

conn_str = 'STRING'
conn_str_for_exam = 'STRING'
container_name = 'container1'

CREATE_CONTAINER = True
ADD_FILE_TO_CONTAINER = True
CHECK_CONTAINER = False
PRINT_ALL_CONTAINERS = False
DEBUG = False

if DEBUG:
    conn = conn_str
else:
    conn = conn_str_for_exam

if __name__ == '__main__':

    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn)
        print(blob_service_client)

    except Exception as e:
        print(f"Can not connect to blob via SAS: {e}")

    if CREATE_CONTAINER:
        try:
            container_client = blob_service_client.create_container(container_name)
        except Exception as e:
            print(f"Can not create container: {e}")


    if ADD_FILE_TO_CONTAINER:
        try:
            upload_file_path = 'Data set'
            blob_client = blob_service_client.get_blob_client(container=container_name, blob='Name')
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)

        except Exception as e:
            print("Can not upload file: {e}")

    if CHECK_CONTAINER:
        try:
            container_client = blob_service_client.get_container_client(container_name)
            container_properties = container_client.get_container_properties()
            print(container_properties)

        except Exception as e:
            print(f"Can not check container: {e}")

    if PRINT_ALL_CONTAINERS:
        try:
            containers = blob_service_client.list_containers()
            k = 1
            for i in containers:
                print(f"{k}. - {i}")
                k += 1

        except Exception as e:
            print(f"Can not get all containers in blob: {e}")
