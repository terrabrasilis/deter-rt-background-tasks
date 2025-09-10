import os
from string import Template

class TemplateLoader():
        
    @staticmethod
    def read_html_template(project_dir:str, file_name:str, **kwargs):
        template_dir = f"{project_dir}/templates"
        with open(os.path.join(template_dir, file_name), 'r') as file:
            template = Template(file.read())
            return template.substitute(**kwargs)