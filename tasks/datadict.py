""" Parse a Salesforce project and generate a data dictionary. """
import os
import xml.etree.ElementTree as ET

import tablib
import attr

from cumulusci.core.tasks import BaseTask


class GenerateDataDictTask(BaseTask):
    """ Generates a data dictionary for the project. """
    task_options = {
        'path': {
            'description': 'The source path to describe.',
            'required': True,
        },
        'output': {
            'description': 'Where to write the xls. Default to datadict.xls', }
    }

    def _init_options(self, kwargs):
        super(GenerateDataDictTask, self)._init_options(kwargs)
        if 'output' not in self.options:
            self.options['output'] = 'datadict.xls'

    def _run_task(self):
        d = ProjectDescriber(source_root=self.options.get('path'))
        with open(self.options.get('output'), 'w') as f:
            f.write(d.xls)
        self.logger.info('Wrote data dictionary.')


@attr.s
class ProjectDescriber(object):
    """ Describe all CustomObjects for a source tree. """
    source_root = attr.ib(default='src/')
    objects = attr.ib(default=attr.Factory(list))

    def __attrs_post_init__(self):
        self._load_objects()

    def _load_objects(self):
        object_names = os.listdir(os.path.join(self.source_root, 'objects'))
        for object_name in object_names:
            full_path = os.path.join(self.source_root, 'objects', object_name)
            with open(full_path) as f:
                contents = f.read()
            d = CustomObjectDescriber(object_name, contents)
            self.objects.append(d.fields)

    @property
    def xls(self):
        book = tablib.Databook(self.objects)
        return book.xls


@attr.s
class CustomObjectDescriber(object):
    """ Generate a description of the fields in a CustomObject. """
    object_file_name = attr.ib()
    object_file_contents = attr.ib(repr=False)
    fields = attr.ib(default=attr.Factory(tablib.Dataset),
                     init=False)  # type: tablib.Dataset
    _root = attr.ib(init=False, repr=False)

    def __attrs_post_init__(self):
        self.fields.headers = (
            'field_name', 'description', 'helptext', 'label')
        self.fields.title = self.object_file_name.split(
            '.')[0][:30]
        self._parse_doc()

    def _parse_doc(self):
        self._root = ET.fromstring(self.object_file_contents)
        for field in self._root.findall('{http://soap.sforce.com/2006/04/metadata}fields'):
            field_name = field.findtext(
                '{http://soap.sforce.com/2006/04/metadata}fullName')
            field_description = field.findtext(
                '{http://soap.sforce.com/2006/04/metadata}description')
            field_helptext = field.findtext(
                '{http://soap.sforce.com/2006/04/metadata}inlineHelpText')
            field_label = field.findtext(
                '{http://soap.sforce.com/2006/04/metadata}label')
            self.fields.append(SalesforceField(
                field_name=field_name, description=field_description,
                helptext=field_helptext, label=field_label))


@attr.s(slots=True)
class SalesforceField(object):
    """ Data representation of a field, to be added as a tablib row. 

    This class is quickly turned into a row, and has no behavior."""
    field_name = attr.ib()
    description = attr.ib()
    helptext = attr.ib()
    label = attr.ib()

    def __len__(self):
        return 4

    def __iter__(self):
        return (col for col in (self.field_name, self.description, self.helptext, self.label))


if __name__ == '__main__':
    import sys
    fname = sys.argv[1]
    describer = ProjectDescriber(fname)

    oname = sys.argv[2]
    with open(oname, 'w') as f:
        f.write(describer.xls)

    print 'Wrote data dict.'
