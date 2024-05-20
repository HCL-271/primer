import os
import time
from datetime import datetime
from stat import S_IFREG
from stream_zip import ZIP_32, stream_zip
from stream_unzip import stream_unzip
import job_pb2

# Настройка gRPC клиента
server_address = 'localhost:50051'
client = RubrClient(server_address)

def create_job(target_file, name):
    with open(target_file, "rb") as f:
        file_content = f.read()

    request = job_pb2.JobRequest(
        job_worker_id="default_worker",  # Замените на подходящий идентификатор
        label="Test Job Encryption New",
        file_name=name,
        file_content=file_content
    )

    response = client.create_job(request)
    if response.status_code != 200:
        raise Exception(f'Failed to create job, status: {response.status_code}')
    return response.job_id

def wait_job_finish(job_id):
    while True:
        response = client.get_job_status(job_id)
        if response.status == job_pb2.JobStatus.IN_PROGRESS:
            print("Job in progress. Waiting 1 second.")
            time.sleep(1)
        else:
            return response

def get_result_docs(job_id, name):
    response = client.get_job_result(job_id)
    if response.status_code != 200:
        raise Exception(f'Failed to get job result, status: {response.status_code}')
    
    with open("output/" + name, "wb") as fp:
        fp.write(response.file_content)
    print("Saved file with documents", name)

def delete_job(job_id):
    response = client.delete_job(job_id)
    if response.status_code != 200:
        raise Exception(f'Failed to delete job: {response.status_code}')
    print(f"Job {job_id} deleted")

# Основная логика аналогична вашему скрипту
if __name__ == "__main__":
    start_time = time.time()

    files = os.listdir("dataset/Исходные")
    root = "dataset/Исходные"
    pull_len = 8
    length = len(files)
    jobs = []
    done = 0
    i = 0
    end = 0

    while done < length:
        if end:
            break
        if i <= length:
            done += 0
        else:
            end = 1
        try:
            while len(jobs) < pull_len:
                if end:
                    break

                if i < length:
                    target_file = os.path.join(root, files[i])
                    print("Поставлена в работу задача ", i)
                    job_id = create_job(target_file, files[i])
                    jobs.append(job_id)
                    i += 1
                else:
                    break

            time.sleep(0.3)

            for job_ids in jobs:
                status = wait_job_finish(job_ids)
                if status.status == job_pb2.JobStatus.IN_PROGRESS:
                    print("Work in progress", job_ids)
                    continue

                if status.status in [job_pb2.JobStatus.CANCELED, job_pb2.JobStatus.FAILED, job_pb2.JobStatus.COMPLETED]:
                    jobs.remove(job_ids)
                    done += 1

                    if len(jobs) > pull_len or end or i >= length:
                        continue
                    job_id = create_job(target_file, files[i])
                    jobs.append(job_id)
                    i += 1

                    if status.status == job_pb2.JobStatus.CANCELED:
                        print("Job canceled")
                    elif status.status == job_pb2.JobStatus.FAILED:
                        print("Job failed")
                    else:
                        print("Job completed")

                print("Completed tasks ", done)
                delete_job(job_ids)
                if end:
                    break

        except Exception as error:
            print("Error", repr(error))

    for job_ids in jobs:
        delete_job(job_ids)

    print("tasks done", done)
    end_time = time.time() - start_time
    print(f"Total execution time - {end_time}")
