from concurrent import futures
import random

import grpc

from recommendations_pb2 import (
    BookCategory,
    BookRecommendation,
    RecommendationResponse,

)

import recommendations_pb2_grpc

books_by_category = {
    BookCategory.MYSTERY: [
         BookRecommendation(id=1, title='Малтийский сокол'),
         BookRecommendation(id=2, title='Убийство в Восточном экспрессе'),
         BookRecommendation(id=3, title='Собака Баскервилей'),
         BookRecommendation(id=4, title='Автостопом по галактике'),
         BookRecommendation(id=5, title='Игра Эндера'),
    ],

    BookCategory.SCIENCE_FICTION: [
        BookRecommendation(id=6, title='Дюна'),
    ],

    BookCategory.SELF_HELP: [
        BookRecommendation(id=7, title='Семь навыков высокоэффективных людей'),
        BookRecommendation(id=8, title='Как завоёвывать друзей и оказать влияние на людей'),
        BookRecommendation(id=9, title='Человек в поисках смысла'),
    ],
}


class RecommendationService(recommendations_pb2_grpc.RecommendationsServicer):
    def Recommend(self, request, context):
        if request.category not in books_by_category:
            context.abort(grpc.StatusCode.NOT_FOUND, "Category not found")

        books_for_category = books_by_category[request.category]
        num_result = min(request.max_results, len(books_for_category))
        books_to_recommend = random.sample(books_for_category, num_result)

        return RecommendationResponse(recommendations=books_to_recommend)
    

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    recommendations_pb2_grpc.add_RecommendationsServicer_to_server(
        RecommendationService(), server
    )

    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()