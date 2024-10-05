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
         BookRecommendation(id=6, title='Тайна Сароса'),
         BookRecommendation(id=7, title='Загадка замка'),
         BookRecommendation(id=8, title='Крестовый поход печатника'),
         BookRecommendation(id=9, title='Девять смертных грехов'),
         BookRecommendation(id=10, title='Человек, который сжег Джульена'),
    ],

    BookCategory.SCIENCE_FICTION: [
        BookRecommendation(id=11, title='Дюна'),
        BookRecommendation(id=12, title='Фoundation'),
        BookRecommendation(id=13, title='Сто лет одиночества'),
        BookRecommendation(id=14, title='Обитель андроида'),
        BookRecommendation(id=15, title='Мир на нитке'),
        BookRecommendation(id=16, title='Космические войны'),
        BookRecommendation(id=17, title='Звездные войны'),
        BookRecommendation(id=18, title='Матрица'),
        BookRecommendation(id=19, title='Гаттерра'),
        BookRecommendation(id=20, title='Незнайка в космосе'),
    ],

    BookCategory.SELF_HELP: [
        BookRecommendation(id=21, title='Семь навыков высокоэффективных людей'),
        BookRecommendation(id=22, title='Как завоёвывать друзей и оказать влияние на людей'),
        BookRecommendation(id=23, title='Человек в поисках смысла'),
        BookRecommendation(id=24, title='Десять дней, которые потрясли мир'),
        BookRecommendation(id=25, title='Искусство быть'),
        BookRecommendation(id=26, title='Принципы эффективности'),
        BookRecommendation(id=27, title='Открытие себя'),
        BookRecommendation(id=28, title='Азбука успеха'),
        BookRecommendation(id=29, title='Духовное руководство'),
        BookRecommendation(id=30, title='Путь к процентному миллиону'),
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