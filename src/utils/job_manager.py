import uuid

class JobManager:
    jobs = {}

    @staticmethod
    def create_job(validation_results):
        job_id = str(uuid.uuid4())
        JobManager.jobs[job_id] = validation_results
        return job_id

    @staticmethod
    def get_job_status(job_id):
        return JobManager.jobs.get(job_id, "Job Not Found")

    @staticmethod
    def list_jobs():
        return list(JobManager.jobs.keys())
