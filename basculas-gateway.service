import json
import queue
import re
import signal
import socket
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import yaml


RUNNING = True


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def parsear_peso(texto):
    """
    Extrae peso y unidad desde una cadena.
    Ejemplos válidos:
      '12.34 kg'
      'ST,GS,+ 0012.50kg'
      'Peso: 450 g'
    """
    match = re.search(r'([-+]?\d+(?:[.,]\d+)?)\s*(g|kg|lb)\b', texto, re.IGNORECASE)
    if not match:
        return None, None

    valor_txt = match.group(1).replace(",", ".")
    unidad = match.group(2).lower()

    try:
        valor = float(valor_txt)
    except ValueError:
        return None, None

    return valor, unidad


class MqttPublisher:
    def __init__(self, host, port, base_topic):
        self.host = host
        self.port = port
        self.base_topic = base_topic.rstrip("/")
        self.client = mqtt.Client()
        self.connected = False
        self.lock = threading.Lock()

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc):
        self.connected = True
        print(f"✅ MQTT conectado a {self.host}:{self.port} rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        print(f"⚠️ MQTT desconectado rc={rc}")

    def connect(self):
        self.client.reconnect_delay_set(min_delay=2, max_delay=30)
        self.client.connect(self.host, self.port, keepalive=60)
        self.client.loop_start()

    def stop(self):
        try:
            self.client.loop_stop()
        finally:
            try:
                self.client.disconnect()
            except Exception:
                pass

    def topic(self, antena_id, leaf):
        return f"{self.base_topic}/{antena_id}/{leaf}"

    def publish_json(self, topic, payload, retain=False, qos=1):
        data = json.dumps(payload, ensure_ascii=False)
        with self.lock:
            info = self.client.publish(topic, data, qos=qos, retain=retain)
        return info


def publicar_estado(pub, antena, status, extra=None):
    payload = {
        "antena_id": antena["id"],
        "ip": antena["ip"],
        "port": antena["port"],
        "status": status,
        "ts": utc_now_iso(),
    }
    if extra:
        payload.update(extra)

    topic = pub.topic(antena["id"], "estado")
    pub.publish_json(topic, payload, retain=True)


def reader_worker(antena, event_queue, pub):
    antena_id = antena["id"]
    ip = antena["ip"]
    port = antena["port"]
    reconnect_seconds = antena.get("reconnect_seconds", 5)
    socket_timeout = antena.get("socket_timeout", 15)

    ultimo_payload = None
    reconnects = 0

    while RUNNING:
        sock = None
        try:
            print(f"🔌 [{antena_id}] Conectando a {ip}:{port} ...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(socket_timeout)
            sock.connect((ip, port))

            print(f"✅ [{antena_id}] Conectado")
            publicar_estado(pub, antena, "online", {"reconnects": reconnects})

            while RUNNING:
                data = sock.recv(1024)
                if not data:
                    raise ConnectionError("conexión cerrada por el dispositivo")

                texto = data.decode("utf-8", errors="ignore").strip()
                if not texto:
                    continue

                valor, unidad = parsear_peso(texto)
                if valor is None:
                    # Opcional: publicar RAW si quieres depurar
                    continue

                payload = {
                    "antena_id": antena_id,
                    "ip": ip,
                    "port": port,
                    "valor": valor,
                    "unidad": unidad,
                    "ts": utc_now_iso(),
                    "raw": texto,
                }

                # Evitar publicar duplicados exactos seguidos
                comparable = (payload["valor"], payload["unidad"])
                if comparable != ultimo_payload:
                    event_queue.put(("peso", antena_id, payload))
                    ultimo_payload = comparable

        except Exception as e:
            reconnects += 1
            print(f"❌ [{antena_id}] Error: {e}")
            publicar_estado(
                pub,
                antena,
                "offline",
                {"error": str(e), "reconnects": reconnects},
            )
            time.sleep(reconnect_seconds)

        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass


def publisher_worker(event_queue, pub):
    while RUNNING:
        try:
            item = event_queue.get(timeout=1)
        except queue.Empty:
            continue

        try:
            tipo, antena_id, payload = item

            if tipo == "peso":
                topic = pub.topic(antena_id, "peso")
                pub.publish_json(topic, payload, retain=True, qos=1)
                print(f"📤 MQTT {topic}: {payload['valor']} {payload['unidad']}")

        except Exception as e:
            print(f"❌ Error publicando MQTT: {e}")
        finally:
            event_queue.task_done()


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def handle_signal(signum, frame):
    global RUNNING
    print(f"🛑 Señal recibida ({signum}), cerrando...")
    RUNNING = False


def main():
    config = load_config("config.yml")

    mqtt_cfg = config["mqtt"]
    base_topic = config.get("base_topic", "basculas")
    antenas = config["antenas"]

    pub = MqttPublisher(
        host=mqtt_cfg["host"],
        port=mqtt_cfg["port"],
        base_topic=base_topic,
    )
    pub.connect()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    event_queue = queue.Queue(maxsize=10000)

    pub_thread = threading.Thread(
        target=publisher_worker,
        args=(event_queue, pub),
        daemon=True,
    )
    pub_thread.start()

    workers = []
    for antena in antenas:
        t = threading.Thread(
            target=reader_worker,
            args=(antena, event_queue, pub),
            daemon=True,
        )
        t.start()
        workers.append(t)

    print("🚀 Gateway arrancado")

    try:
        while RUNNING:
            time.sleep(1)
    finally:
        print("🧹 Cerrando servicio...")
        for antena in antenas:
            try:
                publicar_estado(pub, antena, "offline", {"reason": "service_stopped"})
            except Exception:
                pass
        pub.stop()


if __name__ == "__main__":
    main()