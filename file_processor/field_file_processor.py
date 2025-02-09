from bronze.field_data_validator import validate_field
from file_processor.file_processor import FileProcessor
from silver.field_data_silver_processing import process_field_data_for_silver_zone


class FieldFileProcessor(FileProcessor):

    def validate(self, dataframe, result):
        print("Validating field file...")
        # Add field-specific validation logic
        # Perform field validation
        validate_field(dataframe, result.id, result.filename)

    def process(self):
        print("Processing field file...")
        # Add field-specific processing logic
        process_field_data_for_silver_zone(self.fileId, self.fileName, self.column_list)


