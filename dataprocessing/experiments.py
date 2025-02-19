
import json
import logging
from pathlib import Path
import sqlite3

import pandas as pd
from dataprocessing.write_log import write_app_log


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
    ##### similarity_list = {item: [matches]}
    similarity_list = _get_similarity_list(similarity_file, output_format)
    
    ##### matches = {item: [matches]}
    matches = _get_ground_truth(ground_truth_file)

    correct_matches = 0
    total_predicted_matches = 0
    total_relevant_matches = 0

    # Iterate through similarity_list
    for item in similarity_list:
        # keep only id numbers in similarity list
        predicted_matches = [m for _, m in similarity_list[item]]
        actual_matches = matches.get(item, [])
        
        total_predicted_matches += len(predicted_matches)
        total_relevant_matches += len(actual_matches)
        
        # Count the correct matches (intersection of predicted and actual)
        correct_matches += len(set(predicted_matches) & set(actual_matches))
    
    # print(total_predicted_matches)
    # print(total_relevant_matches)
    # print(correct_matches)
    # Precision: Number of correct matches / Total predicted matches
    precision = correct_matches / total_predicted_matches if total_predicted_matches != 0 else 0.0
    
    # Recall: Number of correct matches / Total relevant matches
    recall = correct_matches / total_relevant_matches if total_relevant_matches != 0 else 0.0

    f1_score = 2*precision*recall / (precision + recall) if (precision + recall) != 0 else 0.0
    
    print(f"Experiment result for {similarity_file}: \n precision: {precision} \n recall: {recall} \n f1 score: {f1_score}")
    Path(f'''{configuration["log_path"]}/experiments''').mkdir(parents=True, exist_ok=True)
    logger = write_app_log(f'''{configuration["log_path"]}/experiments/exp.log''')
    logger.info(f"Experiment result for {similarity_file}: \n precision: {precision} \n recall: {recall} \n f1 score: {f1_score}")
    return precision, recall, f1_score