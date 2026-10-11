"""Disposable real KRaft broker for tests/benchmarks; never uses local Kafka data."""
from __future__ import annotations
import os
import socket
import subprocess
import time
from pathlib import Path
from confluent_kafka.admin import AdminClient
from .admin_topics import process_topics, load_specs, DEFAULT_CONFIG


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class TestBroker:
    __test__ = False

    def __init__(self, directory, kafka_home=None):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.home = Path(kafka_home or os.getenv("KAFKA_HOME", "/opt/kafka"))
        self.port = free_port()
        controller = free_port()
        while controller == self.port:
            controller = free_port()
        self.bootstrap = f"127.0.0.1:{self.port}"
        self.config = self.directory / "server.properties"
        self.config.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{controller}
listeners=PLAINTEXT://127.0.0.1:{self.port},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://127.0.0.1:{self.port}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={self.directory / 'data'}
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
num.network.threads=2
num.io.threads=2
log.retention.hours=1
log.segment.bytes=16777216
''')
        self.env = {**os.environ, "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m", "LOG_DIR": str(self.directory / "logs")}
        self.proc = None
        self.output = None
        storage = str(self.home / "bin/kafka-storage.sh")
        cluster_id = subprocess.check_output([storage, "random-uuid"], env=self.env, text=True).strip()
        subprocess.run([storage, "format", "-t", cluster_id, "-c", str(self.config)], env=self.env,
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)

    def start(self):
        self.output = (self.directory / "broker.out").open("ab")
        self.proc = subprocess.Popen([str(self.home / "bin/kafka-server-start.sh"), str(self.config)],
                                     env=self.env, stdout=self.output, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 60
        admin = AdminClient({"bootstrap.servers": self.bootstrap, "log_level": 0})
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"Test broker exited; inspect {self.directory / 'broker.out'}")
            try:
                if admin.list_topics(timeout=1).brokers:
                    return
            except Exception:
                time.sleep(0.2)
        self.stop()
        raise RuntimeError("Test broker startup timeout")

    def topics(self):
        process_topics(AdminClient({"bootstrap.servers": self.bootstrap}), load_specs(DEFAULT_CONFIG), "apply")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=45)
        if self.output:
            self.output.close()
