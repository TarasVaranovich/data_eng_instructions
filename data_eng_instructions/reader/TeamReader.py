from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

from data_eng_instructions.filedefinition.team.TeamDefinition import TeamDefinition
from data_eng_instructions.reader.EntityReader import EntityReader


class TeamReader(EntityReader[DateType]):

    def __init__(
            self,
            session: SparkSession,
            file_definition: TeamDefinition
    ):
        self.session = session
        self.file_definition = file_definition
        super().__init__(session, file_definition)