
import json
import logging
from pathlib import Path
import sqlite3
import math
import pandas as pd
from dataprocessing.write_log import write_app_log
from scipy.stats import kendalltau

def _get_ground_truth(ground_truth_file):
    matches = {}
    n_lines = 0
    with open(ground_truth_file, 'r', encoding='utf-8') as fp:
        for n, line in enumerate(fp.readlines()):
            if len(line.strip()) > 0:
                item, match = line.replace('_', '__').split(',')
                if item not in matches:
                    matches[item] = [match.strip()]
                else:
                    matches[item].append(match.strip())
                n_lines = n
        if n_lines == 0:
            raise IOError('Matches file is empty. ')
    return matches

def _get_similarity_list(similarity_file, output_format):
    sim_list = {}
    if output_format == "db":
        try:
            if Path(similarity_file).exists:
                conn = sqlite3.connect(similarity_file)
                cursor = conn.cursor()

                table = "matchinglist"
                primary_key = "id"
                value = "similarity"

                cursor.execute(f'SELECT {primary_key}, {value} FROM {table}')
                rows = cursor.fetchall()

                sim_list = {row[0]: json.loads(row[1]) for row in rows}
                conn.close()
            else:
                raise ValueError("[ERROR] Do not find similarity file.")
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
    elif output_format == "json":
        print("parsing json file...")
        try:
            if Path(similarity_file).exists:
                with open(similarity_file, "r", encoding="utf-8") as f:
                    sim_list = json.load(f)
            else:
                raise ValueError("[ERROR] Do not find similarity file.")
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
    elif output_format == "parquet":
        try:
            if Path(similarity_file).exists:
                data = pd.read_parquet(similarity_file)
                sim_list = dict(zip(data['key'], data['value'].apply(json.loads)))
            else:
                raise ValueError("[ERROR] Do not find similarity file.")
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
    return sim_list


def _get_similarity_list(similarity_file, output_format):
    sim_list = {}
    if output_format == "db":
        try:
            if Path(similarity_file).exists:
                conn = sqlite3.connect(similarity_file)
                cursor = conn.cursor()

                table = "matchinglist"
                primary_key = "id"
                value = "similarity"

                cursor.execute(f'SELECT {primary_key}, {value} FROM {table}')
                rows = cursor.fetchall()

                sim_list = {row[0]: json.loads(row[1]) for row in rows}
                conn.close()
            else:
                raise ValueError("[ERROR] Do not find similarity file.")
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
    elif output_format == "json":
        print("parsing json file...")
        try:
            if Path(similarity_file).exists:
                with open(similarity_file, "r", encoding="utf-8") as f:
                    sim_list = json.load(f)
            else:
                raise ValueError("[ERROR] Do not find similarity file.")
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
    elif output_format == "parquet":
        try:
            if Path(similarity_file).exists:
                data = pd.read_parquet(similarity_file)
                sim_list = dict(zip(data['key'], data['value'].apply(json.loads)))
            else:
                raise ValueError("[ERROR] Do not find similarity file.")
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
    return sim_list


def get_match_pairs(data_dict):
    '''
    param item : {'key': ['match1', 'match2'], 'key2': ['match1', 'match2']}
    return (item, match1), (item, match2)
    '''
    matchpair_set = set()
    for key in data_dict:
        item_matched = int(key.split('__')[1])
        for value in data_dict[key]:
            item_matching = None
            ### value ex. ['idx__27463', 'idx__19499', 'idx__18194', 'idx__14572', 'idx__64547', 'idx__35496', 'idx__22535']
            if isinstance(value, str):
                item_matching = value
            item_matching = int(item_matching.split('__')[1])

            ### sort: facilitate deplication
            if item_matching > item_matched:
                el = (item_matched, item_matching)
            else:
                el = (item_matching, item_matched)
            matchpair_set.add(el)
    return matchpair_set

def get_similar_pairs(data_dict, n, appr):
    '''
    param item : {'key': ['match1', 'match2'], 'key2': ['match1', 'match2']}
    return (item, match1), (item, match2)
    '''
    matchpair_set = set()
    for key in data_dict:
        item_matched = int(key.split('__')[1])
        values = [[round(degree, appr), _] for degree, _ in data_dict[key]]
        if isinstance(n, int) and n>0:
            # find the n most matching scores
            scores = list(set(degree for degree, _ in values))
            scores = sorted(scores, reverse=True)[:n]
            # print(scores)
            values = [value for value in values if value[0] >= scores[len(scores)-1]]
        for item in values:
            item_matching = None
            ### value ex. [[0.8194172382354736, 'idx__38260'], [0.8130440711975098, 'idx__51652']]
            if isinstance(item, list):
                item_matching = item[1]
                print(item_matching)
                item_matching = int(item_matching.split('__')[1])

            ### sort: facilitate deplication
            if item_matching > item_matched:
                el = (item_matched, item_matching)
            else:
                el = (item_matching, item_matched)
            matchpair_set.add(el)
    return matchpair_set

def compare_ground_truth(configuration):
    """
    Test the accuracy of matches by
    :param similarity_file:
    :param ground_truth_file:
    :param n_items:
    :param n_top:
    """
    ground_truth_file = configuration['match_file']
    similarity_file = configuration['similarity_file']
    output_format = configuration['output_format']
    ### similarity_list = {item: [matches]}
    similarity_list = _get_similarity_list(similarity_file, output_format)
    
    ### matches = {item: [matches]}
    matches = _get_ground_truth(ground_truth_file)

    correct_matches = 0
    # correct_pretrained_matches = 0
    predicted_matches = get_similar_pairs(similarity_list, int(configuration['n_first']), int(configuration['approximate']))
    actual_matches = get_match_pairs(matches)
    
    # if 'pretrained_number' in configuration:
    #     pretrain = int(configuration['pretrained_number'])
    # else:
    #     pretrain = 0

    total_predicted_matches = len(predicted_matches)
    print(total_predicted_matches)
    total_relevant_matches = len(actual_matches)
    print(total_relevant_matches)
    correct_matches =  len(set(predicted_matches) & set(actual_matches)) 
    print(correct_matches)

    # Precision: Number of correct matches / Total predicted matches
    precision = correct_matches / total_predicted_matches if total_predicted_matches != 0 else 0.0
    # Recall: Number of correct matches / Total relevant matches
    recall = correct_matches / total_relevant_matches if total_relevant_matches != 0 else 0.0
    f1_score = 2*precision*recall / (precision + recall) if (precision + recall) != 0 else 0.0

    ##### print results
    print(f'''Experiment result for {similarity_file}: \n correct matches: {correct_matches} \n total number of predicted matches: {total_predicted_matches} \n total number of matches in groud truth file: {total_relevant_matches} \n \n precision: {precision} \n recall: {recall} \n f1 score: {f1_score}''')

    ##### output results to log file
    Path(f'''{configuration["log_path"]}/experiments''').mkdir(parents=True, exist_ok=True)
    logger = write_app_log(f'''{configuration["log_path"]}/experiments/''')
    
    logger.info(f'''[RESULTS] Experiment result of similarity list in file {similarity_file} by taking first {configuration["n_first"]} data and an approximate to {configuration['approximate']} decimal places: \n correct matches: {correct_matches} \n total number of predicted matches: {total_predicted_matches} \n total number of matches in groud truth file: {total_relevant_matches} \n \n precision: {precision} \n recall: {recall} \n f1 score: {f1_score}''')
    
    # if correct_pretrained_matches != 0:
    #     p_precision = correct_pretrained_matches / total_predicted_matches if total_predicted_matches != 0 else 0.0
    #     # Recall: Number of correct matches / Total relevant matches
    #     p_recall = correct_pretrained_matches / total_relevant_matches if total_relevant_matches != 0 else 0.0
    #     p_f1_score = 2*precision*recall / (precision + recall) if (precision + recall) != 0 else 0.0

    #     print(f'''Experiment result for pre-trained data in file {similarity_file}: \n correct matches: {correct_pretrained_matches} \n total number of predicted matches: {total_predicted_matches} \n total number of matches in groud truth file: {total_relevant_matches} \n \n precision: {p_precision} \n recall: {p_recall} \n f1 score: {p_f1_score}''')
    #     logger.info(f'''[RESULTS] Experiment result for pre-trained data in file {similarity_file}: \n correct matches: {correct_pretrained_matches} \n total number of predicted matches: {total_predicted_matches} \n total number of matches in groud truth file: {total_relevant_matches} \n \n precision: {p_precision} \n recall: {p_recall} \n f1 score: {p_f1_score}''')

    return precision, recall, f1_score

def _get_dict_to_compare(input_list, configuration):
    output_dict = {}
    for el in input_list:
        id = el.split('__')[1]
        if int(id) > int(configuration["source_num"]):
            output_dict[el]= input_list[el]
    return output_dict

def compare_batch(configuration):
    similarity_list = _get_similarity_list(configuration['similarity_file'], "db")
    batch_list = _get_similarity_list(configuration['batch_file'], "db")

    sim_to_compare = _get_dict_to_compare(similarity_list, configuration)
    batch_to_compare = _get_dict_to_compare(batch_list, configuration)

    # Intersection Matching
    num_nan = 0
    num_tau = 0
    total_tau = 0
    p_value_ok = 0
    positive = 0
    for key in sim_to_compare:
        if key in batch_to_compare:
            
            # 提取元素的标识（如字符串部分）
            sim_id = [x[1] for x in sim_to_compare[key]]
            batch_id = [x[1] for x in batch_list[key]]

            # 取共同元素
            common_labels = set(sim_id) & set(batch_id)

            # 过滤出共同元素并保持原顺序
            sim_filtered = [x for x in sim_id if x in common_labels]
            batch_filtered = [x for x in batch_id if x in common_labels]

            # 建立索引映射，确保一致性
            sim_map = {label: rank for rank, label in enumerate(sim_filtered)}
            batch_map = {label: rank for rank, label in enumerate(batch_filtered)}
            

            # 按相同顺序获取排名列表
            sim_rank = [sim_map[label] for label in sim_filtered]
            batch_rank = [batch_map[label] for label in sim_filtered]

            # 计算 Kendall's Tau 相关系数
            tau, p_value = kendalltau(sim_rank, batch_rank)

            if math.isnan(float(tau)):
                num_nan = num_nan + 1
            else:
                num_tau = num_tau + 1
                total_tau += tau
                if p_value < 0.5 and math.isnan(float(p_value)) == False:
                    p_value_ok = p_value_ok + 1
                    if tau > 0:
                        positive = positive + 1
                    
                    print(key)
                    print(sim_map)
                    print(batch_map)
                    print(f"tau: {tau} \n p_value: {p_value}")
    average_tau = total_tau/num_tau
            
    print(f"average tau: {average_tau}, number of nan : {num_nan}, p_value : {p_value_ok}, positive: {positive}")
    # Path(f'''{configuration["log_path"]}/experiments''').mkdir(parents=True, exist_ok=True)
    # logger = write_app_log(f'''{configuration["log_path"]}/experiments/''')
    
    # logger.info(f"Kendall's Tau for file {configuration['similarity_file']} \n tau: {tau} \n p_value: {p_value}")
    