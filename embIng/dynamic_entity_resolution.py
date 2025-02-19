

def dynentity_resolution(model, target):
    filtered_keys = [word for word in model.wv.index_to_key if word.startswith("idx__")] # only search words beginning with "idx__"
    sims = [(word, score) for word, score in model.wv.most_similar(target, topn=50) if word in filtered_keys and score >0.8][:10]
    # sims = model.wv.most_similar(target, topn=10, restrict_vocab=len(filtered_keys))  # get other similar words
    return sims

# if __name__ == '__main__':
#     configuration = read_configuration('/home/zhongwei/Data_ingestion/embIng/config/config-stream')
#     embeddings_file = "pipeline/embeddings/papers.emb"
#     model = Word2Vec.load(embeddings_file) 
#     dynentity_resolution(model, configuration, 'test', None)