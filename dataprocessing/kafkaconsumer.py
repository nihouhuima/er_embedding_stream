import json
import pathlib
from threading import Timer
import pandas as pd
import time
from kafka import KafkaConsumer
from collections import deque
from embIng.dynamic_edges import dynedges_generation
from embIng.dynamic_sentence_generation_strategies import dynrandom_walks_generation
from embIng.dynamic_entity_resolution import dynentity_resolution
from dataprocessing.similaritylist import SimilarityList
from dataprocessing.write_log import write_app_log, write_kafka_log

def check_configuration(config):
    """Validate configuration parameters"""
    required_params = [
        'kafka_topicid', 'bootstrap_servers', 'port',
        'window_strategy', 'update_frequency',
        'most_similar_inlist_n', 'output_format', 'kafka_groupid'
    ]
    
    for param in required_params:
        if param not in config:
            raise ValueError(f"Missing required configuration parameter: {param}")
        else:
            config["update_frequency"] = int(config["update_frequency"])
            config["most_similar_k"] = int(config["most_similar_k"])
            config["most_similar_inlist_n"] = int(config["most_similar_inlist_n"])
            config["show_m_most_similar"] = int(config["show_m_most_similar"])
            
    if config["window_strategy"] not in ["count", "time"]:
        raise ValueError("Expected sliding window strategy, pls choose between [\"count\", \"time\"]")
    elif config["window_strategy"] == "count":
        try:
            config["window_count"] = int(config["window_count"])
        except ValueError:
            raise ValueError("Expected window_count value.")
    elif config["window_strategy"] == "time":
        try:
            config["window_time"] = int(config["window_time"])
        except ValueError:
            raise ValueError("Expected window_count value.")
    
    if config["output_format"] not in ["db", "json", "parquet"]:
        raise ValueError("output_format must be one of ['db', 'json', 'parquet']")

    return config
    
class ConsumerService:
    def __init__(self, configuration, graph, model, edgelist_path, embeddings_file, prefixes, id_num):
        self.config = configuration
        self.graph = graph
        self.model = model
        self.edgelist_path = edgelist_path
        self.embeddings_file = embeddings_file
        self.prefixes = prefixes
        self.id_num = id_num
        self.sim_list = SimilarityList(
                            configuration["most_similar_inlist_n"],
                            configuration["output_format"]
                        )
        self.window_data = deque()
        self.last_update_time = time.time()
        self.data_buffer = []
        self.flag_running = False
        self.write_timer = None
        self.time_interval = 2 # 300 seconds = 5 minutes
        self.app_logger = None
        self.kafka_logger = None
        self.setup_logging()

    def setup_logging(self):
        self.app_logger = write_app_log(self.config["log_path"])
        self.kafka_logger = write_kafka_log(self.config["log_path"])
        self.sim_list.set_logger(self.app_logger)

    def trigger_file_write(self):
        name = pathlib.Path(self.embeddings_file).stem
        print("name: ", name)
        self.sim_list.check_output_path(name)
        if self.flag_running:
            self.write_timer = Timer(self.time_interval, self.trigger_file_write)  
            self.write_timer.daemon = True  # Make it a daemon thread
            self.write_timer.start()
        self.sim_list.update_file()

    def remove_expired_data(self, current_time):
        """Remove data older than the window size"""
        while self.window_data and (current_time - self.window_data[0]["timestamp"] > self.config["window_time"]):
            self.window_data.popleft()

    def prepare_data(self, window_data):
        # construct data structucre
        data_list = list(window_data)
        # print("list of window data: ", data_list)
        df = pd.DataFrame.from_records(data_list)
        # print("df of window data: ", df)

        # df = pd.DataFrame(columns=["author", "language", "title"])
        # df.loc[len(df)] = [metadata.get("author"), metadata.get("language"), metadata.get("title")]
        return df

    def build_matching_list(self, df):
        print("build similarity list...")
        # get similar words 
        for target in df.loc[:,"rid"]:
            # print("target node: ", target)
            try:
                similar = dynentity_resolution(self.model, target)
                if similar != [] and similar is not None:
                    self.sim_list.add_similarity(target, similar)

                    if self.sim_list.output_format == "db":
                        self.sim_list.insert_data(target, similar)
                    # print(self.sim_list.get_similarity_words_with_score(target, self.config["show_m_most_similar"]))
                    for word, score in similar:
                        self.sim_list.add_similarity(word, [(target, score)])
                        if self.sim_list.output_format == "db":
                            self.sim_list.insert_data(word, [(target, score)])
                else:
                    pass
                # print(f"Can't find similar result for {target}: " , Exception)
            except Exception:
                pass
            # print("similar nodes: ", similar)

    def process_window_data(self):
        """Process the current window of data"""
        print("processing window data...")
        df = self.prepare_data(self.window_data)
        if self.config["update_frequency"] == 0:
            self.window_data.clear()

        # add new node to graph
        edgelist = dynedges_generation(df, self.edgelist_path, self.prefixes, self.config["smoothing_method"])
        self.graph.update_graph(list(edgelist), sim_list=None, delta=True)
        # print("delta_nodes: ", self.graph.delta_nodes)
        # print("delta_cell_lists: ", self.graph.delta_cell_lists)
        # start random walk for new data
        walks = dynrandom_walks_generation(self.config, self.graph)
        
        # Update model
        try:
            self.model.build_vocab(walks, update=True)
            self.model.train(walks, total_examples=len(walks), epochs=5) # An epoch is one complete pass through the entire training data.
        except Exception as e:
            print("[ERROR]: ", e)
            self.model.build_vocab(walks)
            self.model.train(walks, total_examples=self.model.corpus_count, epochs=10)
        self.build_matching_list(df)

    def run(self):
        if self.sim_list.output_format != "db":
            self.flag_running = True
            self.trigger_file_write()
        else:
            name = pathlib.Path(self.embeddings_file).stem
            print("name: ", name)   
            self.sim_list.check_output_path(name)
        try:
            # prepare output 
            # print(f"test: {self.embeddings_file}")         
            
            t_start = time.time()
            consumer = KafkaConsumer(
                self.config['kafka_topicid'],
                bootstrap_servers=f'{self.config["bootstrap_servers"]}:{self.config["port"]}',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id=self.config["kafka_groupid"],
                # auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            self.app_logger.info("Start Kafka consumer...")
            print("Start Kafka consumer...")
            
            for msg in consumer:
                try:
                    self.id_num += 1
                    metadata = msg.value
                    metadata["rid"] = f"idx__{self.id_num}"
                    current_time = time.time()
                    
                    # Add data to date buffer
                    self.data_buffer.append(metadata)

                    if self.config["window_strategy"] == "time":
                        metadata["timestamp"] = current_time
                        self.window_data.append(metadata)
                        
                        if self.config["update_frequency"] != 0:
                            self.remove_expired_data(current_time)
                        
                        if current_time - self.last_update_time >= self.config["update_frequency"]:
                            self.process_window_data()
                            self.last_update_time = current_time
                    
                    elif self.config["window_strategy"] == "count":
                        self.window_data.append(metadata)
                        # print("window data len", len(self.window_data))
                        if len(self.window_data) == self.config["window_count"]:
                            self.process_window_data()
                            
                            if self.config["update_frequency"] != 0:
                                for _ in range(self.config["update_frequency"]):
                                    if self.window_data:
                                        self.window_data.popleft()
                            else:
                                self.window_data.clear()
                    

                                
                except Exception as e:
                    self.app_logger.error(f"Error processing message: {str(e)}")
                    continue
                    
        except Exception as e:
            self.app_logger.error(f"Fatal error in consumer service: {str(e)}")
        # finally:
        #     consumer.close()
        #     logging.info("Consumer service stopped")
           

if __name__ == '__main__':
    # consumer_service(configuration, graph, wv)
    pass