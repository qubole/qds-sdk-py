import json

class CmdLine:
    @staticmethod
    def filter_fields(resource, fields):
        filtered = {}
        for field in fields:
            filtered[field] = resource[field]
        return filtered

    @staticmethod
    def create(cls, args):
        with open(args.data) as f:
            spec = json.load(f)
        schedule = cls(spec)
        return json.dumps(resource.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(cls, args):
        resource_list = cls.list(args.page, args.per_page)
        if args.fields:
            for s in resource_list:
                s.attributes = CmdLine.filter_fields(s.attributes, args.fields)
        return json.dumps(resource_list, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def view(cls, args):
        cls_instance = cls.find(args.id)
        if args.fields:
            cls_instance.attributes = CmdLine.filter_fields(cls_instance.attributes, args.fields)
        return json.dumps(cls_instance.attributes, sort_keys=True, indent=4)

    @staticmethod
    def delete(cls, args):
        return json.dumps(cls.delete(args.id), sort_keys=True, indent=4)
