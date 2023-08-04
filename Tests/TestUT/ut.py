from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import subprocess

class TestAT:
    def __init__(self):
        self.spark = SparkSession.builder.appName("TestAT").getOrCreate()

    def run_process(self, script_path):
        cmd = ["spark-submit", script_path]
        result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        return result.stdout

    def test_exit_code(self, script_path):
        result = self.run_process(script_path)
        if result.returncode == 0:
            print("Prueba de Aceptación 1: Exit Code is Zero - PASS")
        else:
            print("Prueba de Aceptación 1: Exit Code is Non-Zero - FAIL")

    def test_duration_in_hours(self, script_path):
        result_df = self.run_process(script_path)
        if 'duration_in_hours' in result_df.columns:
            print("Prueba de Aceptación 2: Duration in Hours Column Found - PASS")
        else:
            print("Prueba de Aceptación 2: Duration in Hours Column Not Found - FAIL")

    def test_no_null_values(self, script_path):
        result_df = self.run_process(script_path)
        null_count = result_df.select([f.count(f.when(f.isnull(c), c)).alias(c) for c in result_df.columns]).collect()
        if all(count == 0 for count in null_count[0]):
            print("Prueba de Aceptación 3: No Null Values - PASS")
        else:
            print("Prueba de Aceptación 3: Null Values Found - FAIL")

    def run_acceptance_tests(self, script_path):
        self.test_exit_code(script_path)
        self.test_duration_in_hours(script_path)
        self.test_no_null_values(script_path)

    def close(self):
        self.spark.stop()


if __name__ == "__main__":
    acceptance_tests = TestAT()
    script_path = "trys/transforms/transforms.py"
    acceptance_tests.run_acceptance_tests(script_path)
    acceptance_tests.close()