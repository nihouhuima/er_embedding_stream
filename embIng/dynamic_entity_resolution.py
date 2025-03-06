import faiss
from gensim.similarities.annoy import AnnoyIndexer
import numpy as np



class FaissIndex:
    def __init__(self, model):
        self.filtered_word_to_idx = {}  # 记录 "idx__" 开头的 word 到 FAISS index 的映射
        self.idx_to_filtered_word = {}  # 记录 FAISS index 到 word 的映射

        dimension = model.vector_size
        self.index = faiss.IndexFlatIP(dimension)
        # self.index = faiss.IndexFlatL2(dimension)

        word_vectors = []
        idx = 0  # FAISS 内部索引
        for word in model.wv.index_to_key:
            if word.startswith("idx__"):
                vector = model.wv[word]
                word_vectors.append(vector)
                self.filtered_word_to_idx[word] = idx
                self.idx_to_filtered_word[idx] = word
                idx += 1
        
        if word_vectors:
            mat = np.array(word_vectors, dtype=np.float32)
            faiss.normalize_L2(mat)
            self.index.add(mat)  # add to FAISS
        

    def get_similar_words(self, query_vector_model, query_word, top_k=5):
        """查询 FAISS 获取相似的 'idx__' 开头的词，并返回相似度分数"""
        if query_word not in self.filtered_word_to_idx:
            return None
        # print('length of index', self.index.ntotal)

        query_vector = np.array(query_vector_model, dtype=np.float32)
        faiss.normalize_L2(query_vector)
        distances, indices = self.index.search(query_vector, int(top_k)*10)

        # print("find number", len(distances))
        similar_words = []
        # FAISS 返回的是 L2 距离，需要转换成相似度c
        # 余弦相似度 = 1 / (1 + L2距离)
        # print(indices)
        for dist, idx in zip(distances[0], indices[0]) :
            similar_word  = self.idx_to_filtered_word[idx]
            # print(similar_word)
            if similar_word != query_word and idx in self.idx_to_filtered_word and float(dist) > 0.5:
                # similar_words = [(similar_word, 1 / (1 + dist))]  # 转换成相似度
                similar_words.append((similar_word,  float(dist)))

        return similar_words


    def update_index(self, model):
        """update FAISS index"""
        new_words = [w for w in model.wv.index_to_key if w not in self.filtered_word_to_idx and w.startswith("idx__")]
        if not new_words:
            return

        new_vectors = np.array([model.wv[w] for w in new_words], dtype=np.float32)
        faiss.normalize_L2(new_vectors)

        self.index.add(new_vectors)

        # update map
        current_size = len(self.filtered_word_to_idx)
        for i, word in enumerate(new_words):
            self.filtered_word_to_idx[word] = current_size + i
            self.idx_to_filtered_word[current_size + i] = word


def dynentity_resolution(model, target, n):
    filtered_keys = [word for word in model.wv.index_to_key if word.startswith("idx__")] # only search words beginning with "idx__"
    sims = [(word, score) for word, score in model.wv.most_similar(target, topn=n*10) if word in filtered_keys and score >0.5][:n]
    # sims = [(word, score) for word, score in model.wv.most_similar(target, topn=n*10) if word in filtered_keys][:n]
    # sims = model.wv.most_similar(target, topn=10, restrict_vocab=len(filtered_keys))  # get other similar words
    return sims   
    


# if __name__ == '__main__':
#     configuration = read_configuration('/home/zhongwei/Data_ingestion/embIng/config/config-stream')
#     embeddings_file = "pipeline/embeddings/papers.emb"
#     model = Word2Vec.load(embeddings_file) 
#     dynentity_resolution(model, configuration, 'test', None)