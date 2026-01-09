import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.kafka import ReadFromKafka
import json

# ---------------- CONFIG ----------------
# These will now be passed as parameters or via GCS
DEFAULT_KAFKA_BOOTSTRAP = "vitalstream-kafka-stephen-c769.d.aivencloud.com:16892"
DEFAULT_FIRESTORE_COLLECTION = "alerts"
DEFAULT_VITALS_TOPIC = "patient_vitals"
DEFAULT_THRESHOLDS_TOPIC = "patient_thresholds"

# GCS paths for service account and Kafka SSL certs
FIREBASE_KEY_GCS = "gs://vitalstream-dataflow-temp/firebase/serviceAccount.json"
KAFKA_CA_GCS = "gs://vitalstream-dataflow-temp/kafka/ca.pem"
KAFKA_CERT_GCS = "gs://vitalstream-dataflow-temp/kafka/service.cert"
KAFKA_KEY_GCS = "gs://vitalstream-dataflow-temp/kafka/service.key"

# ---------------- HELPERS ----------------
def parse_vitals(record):
    """Parse Kafka vitals message into KV"""
    key, value = record
    data = json.loads(value.decode("utf-8"))
    return data["patient_id"], data

def parse_thresholds(record):
    """Parse Kafka thresholds message into KV"""
    key, value = record
    data = json.loads(value.decode("utf-8"))
    return data["patient_id"], data

def evaluate_alert(element):
    """Apply alert logic to joined streams"""
    patient_id, grouped = element
    vitals_list = grouped["vitals"]
    thresholds_list = grouped["thresholds"]

    if not vitals_list or not thresholds_list:
        return None

    v = vitals_list[-1]  # latest vitals
    t = thresholds_list[-1]  # latest thresholds

    status = "CRITICAL" if (
        v["heart_rate"] > t["max_heart_rate"]
        or v["spo2"] < t["min_spo2"]
    ) else "NORMAL"

    return {
        "patient_id": patient_id,
        "heart_rate": v["heart_rate"],
        "spo2": v["spo2"],
        "status": status,
        "timestamp": v["timestamp"]
    }

# ---------------- FIRESTORE DOFN ----------------
class WriteToFirestore(beam.DoFn):
    def __init__(self, firestore_collection, service_account_gcs):
        self.firestore_collection = firestore_collection
        self.service_account_gcs = service_account_gcs

    def setup(self):
        import firebase_admin
        from firebase_admin import credentials, firestore
        from apache_beam.io.gcp import gcsio

        # Copy the key from GCS to local tmp path for the worker
        gcs = gcsio.GcsIO()
        tmp_key_path = "/tmp/serviceAccount.json"
        with gcs.open(self.service_account_gcs) as f_in, open(tmp_key_path, "wb") as f_out:
            f_out.write(f_in.read())

        cred = credentials.Certificate(tmp_key_path)
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    def process(self, element):
        if element is None:
            return
        self.db.collection(self.firestore_collection).document(element["patient_id"]).set(element)
        yield element

# ---------------- PIPELINE ----------------
def run():
    # ---------------- PIPELINE OPTIONS ----------------
    options = PipelineOptions(
        streaming=True,
        save_main_session=True,
    )
    options.view_as(SetupOptions).save_main_session = True

    # Use runtime parameters for flexibility
    vitals_topic = options.get_all_options().get("vitals_topic", DEFAULT_VITALS_TOPIC)
    thresholds_topic = options.get_all_options().get("thresholds_topic", DEFAULT_THRESHOLDS_TOPIC)
    firestore_collection = options.get_all_options().get("alerts_collection", DEFAULT_FIRESTORE_COLLECTION)
    kafka_bootstrap = options.get_all_options().get("kafka_bootstrap", DEFAULT_KAFKA_BOOTSTRAP)

    kafka_config = {
        "bootstrap.servers": kafka_bootstrap,
        "security.protocol": "SSL",
        "ssl.ca.location": KAFKA_CA_GCS,
        "ssl.certificate.location": KAFKA_CERT_GCS,
        "ssl.key.location": KAFKA_KEY_GCS
    }

    with beam.Pipeline(options=options) as p:
        # ---------------- READ VITALS ----------------
        vitals = (
            p
            | "ReadVitals" >> ReadFromKafka(
                topics=[vitals_topic],
                consumer_config=kafka_config
            )
            | "ParseVitals" >> beam.Map(parse_vitals)
        )

        # ---------------- READ THRESHOLDS ----------------
        thresholds = (
            p
            | "ReadThresholds" >> ReadFromKafka(
                topics=[thresholds_topic],
                consumer_config=kafka_config
            )
            | "ParseThresholds" >> beam.Map(parse_thresholds)
        )

        # ---------------- JOIN STREAMS ----------------
        joined = (
            {"vitals": vitals, "thresholds": thresholds}
            | "JoinStreams" >> beam.CoGroupByKey()
        )

        # ---------------- ALERT LOGIC ----------------
        alerts = (
            joined
            | "EvaluateAlert" >> beam.Map(evaluate_alert)
            | "RemoveNulls" >> beam.Filter(lambda x: x is not None)
        )

        # ---------------- WRITE TO FIRESTORE ----------------
        alerts | "WriteToFirestore" >> beam.ParDo(
            WriteToFirestore(firestore_collection, FIREBASE_KEY_GCS)
        )

if __name__ == "__main__":
    run()
