from yargy import (
    rule, or_, and_, not_
)
from yargy.predicates import (
    eq, normalized,
    dictionary,
    custom, gram
)

from yargy.interpretation import (
    fact,
    attribute
)

from yargy.pipelines import morph_pipeline, pipeline
from yargy.relations import gnc_relation

# представление менеджера (first - имя, second - возможная фамилия)
Introduce = fact(
    "Introduce",
    ["first", attribute("second", None)]
)

# вступительные слова
IN_FRONT_NAME = morph_pipeline([
    "я",
    "мое имя",
    "меня зовут",
    "разрешите представиться",
    "вас беспокоит",
    "это"
])

# список имен (тут для примера несколько, нужно расширить)
NAME = dictionary({
    "иван",
    "ангелина",
    "федор",
    "екатерина",
    "василий",
    "максим",
    "сергей",
    "дмитрий",
    "виктория",
    "анастасия",
})

# наиболее распространенные окончания фамилий
family_ends = ["ов", "ова", "ев", "ева",
               "ин", "ина", "ын", "ына", "кий", "кая"]


def is_family(token:str):
    if len(token) < 5:
        return False
    for end in family_ends:
        if token.endswith(end):
            return True

# проверяет является ли токен фамилией
FAMILY = custom(is_family)

# должность менеджера
POSITION = dictionary({
    "сотрудник",
    "специалист",
    "главный специалист",
    "менеджер",
    "главный менеджер",
    "руководитель",
    "представитель",
})

# часть места работы, например, отдел
PART_PLACE_WORK = and_(or_(gram("NOUN"), gram(
    "ADJF"), gram("PREP")), not_(or_(NAME, FAMILY)))

# например, отдел закупок
PLACE_WORK = PART_PLACE_WORK.repeatable(max=3)

# например, специалист отдела закупок
FULL_POSITION = rule(POSITION, PLACE_WORK.optional())

# согласование имени и фамилии в роде, числе, падеже
gnc = gnc_relation()

FIO = or_(
    rule(NAME.interpretation(Introduce.first.inflected()).match(gnc),
         FAMILY.optional().interpretation(Introduce.second.inflected()).match(gnc)),
    rule(FAMILY.interpretation(Introduce.second.inflected()).match(gnc)), NAME.interpretation(Introduce.first.inflected()).match(gnc))


INTRODUCE = rule(
    IN_FRONT_NAME,
    FULL_POSITION.optional(),
    FIO,
    normalized('звать').optional()
).interpretation(Introduce)

# приветствие
Greeting = fact(
    "Greeting",
    ["words"]
)

GREETING = morph_pipeline({
    "здравствуйте",
    "добрый день",
    "доброе утро",
    "доброе вечер",
    "приветствую",
    "приветствую вас",
    "привет",
    "вам звонит",
}).interpretation(Greeting.words).interpretation(Greeting)

# факт названия компании
Company = fact(
    "Company",
    ["name"]
)

# предположим, что имя компании максимум из 2 слов (плюс опционально союз "и" в середине)
PART_COMPANY = rule(or_(gram("NOUN"), gram("ADJF")))
NAME_COMPANY = rule(
    PART_COMPANY,
    or_(
        rule(eq("и"), PART_COMPANY),
        PART_COMPANY
    ).optional()
).interpretation(Company.name)

COMPANY = rule(normalized("компания"), NAME_COMPANY).interpretation(Company)

# поиск где менеджер попрощался
Bye = fact(
    "Bye",
    ["words"]
)

SHORT_BYE = pipeline({
    "счастливо",
    "удачи",
    "всего хорошего",
    "всего доброго",
    "хорошего дня",
    "хорошего вечера",
    "удачного дня"
})

DATES = dictionary({
    "понедельник",
    "вторник",
    "среда",
    "четверг",
    "пятница",
    "суббота",
    "воскресение",
    "скорого",
    "свидания",
    "встречи"
})

# правило для окончания диалога, включает поиск где менеджер попрощался
BYE = or_(
    rule(eq("до"),DATES),
    SHORT_BYE
    ).repeatable().interpretation(Bye.words).interpretation(Bye)

def get_type(fact):
    if isinstance(fact, Greeting):
        return "greeting"
    elif isinstance(fact, Introduce):
        return "introduce"
    elif isinstance(fact, Company):
        return "company"
    elif isinstance(fact, Bye):
        return "bye"
    return None

# прокси факт для объединения нескольких подзадач в одну задачу:
# например [GREETING,INTRODUCE,COMPANY] для парсинга одновременно
Task = fact("Task", ["value"])

def make_task(rules):
    return or_(*rules).interpretation(Task.value).interpretation(Task)
