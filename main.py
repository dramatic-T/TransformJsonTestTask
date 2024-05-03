from extract_repos import RepositoryExtractor
from transform_jsons import SparkJsonTransformer

if __name__ == '__main__':
    repository_extractor = RepositoryExtractor()
    repository_extractor.extract('Scytale-exercise')
    spark_json_transformer = SparkJsonTransformer()
    spark_json_transformer.transform()