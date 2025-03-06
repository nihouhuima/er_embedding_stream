import os

from embIng.embeddings_quality import embeddings_quality
from embIng.entity_resolution import entity_resolution
from embIng.write_logging import *
from embIng.schema_matching import schema_matching
from embIng.utils import remove_prefixes
from dataprocessing.kafkaconsumer import ConsumerService, check_configuration
from dataprocessing.experiments import compare_ground_truth, compare_batch


def test_driver(embeddings_file, configuration=None):
    test_type = configuration['experiment_type']
    info_file = configuration['dataset_info']
    if test_type == 'EQ':
        # print('#'*80)
        print('# EMBEDDINGS QUALITY')
        if configuration['training_algorithm'] == 'fasttext':
            newf = embeddings_file
            mem_results.res_dict = embeddings_quality(newf, configuration)
        else:
            newf = remove_prefixes(configuration['edgelists_file'], embeddings_file)
            mem_results.res_dict = embeddings_quality(newf, configuration)
            os.remove(newf)
    elif test_type == 'ER':
        print('# ENTITY RESOLUTION')

        mem_results.res_dict = entity_resolution(embeddings_file, configuration, info_file=info_file)
    elif test_type == 'SM':
        print('# SCHEMA MATCHING')
        mem_results.res_dict = schema_matching(embeddings_file, configuration)
    else:
        raise ValueError('Unknown test type.')


def match_driver(embeddings_file, df, configuration):
    test_type = configuration['experiment_type']
    info_file = configuration['dataset_info']
    print('Extracting matched tuples')
    m_tuples = entity_resolution(embeddings_file, configuration, info_file=info_file,
                                 task='match')
    # print('Extracting matched columns')
    # m_columns = match_columns(df, embeddings_file)

    return m_tuples, []

def stream_driver(configuration, graph, model, edgelist_path, embeddings_file, prefixes, metrics):    
    try:
        check_configuration(configuration)
    except Exception as e:
        print(f"[ERROR] Can not upload configuration: {e}")
    finally:
        print('Streaming...')
        if int(graph.num_ids) == 0:
            id_num = -1
        else: 
            id_num = int(graph.num_ids)
        consumer = ConsumerService(configuration, graph, model, edgelist_path, embeddings_file, prefixes, id_num, metrics)
        consumer.run()

def experiment_driver(configuration):
    if configuration['kendall'] == "true":
        compare_batch(configuration)
    else:
        compare_ground_truth(configuration)