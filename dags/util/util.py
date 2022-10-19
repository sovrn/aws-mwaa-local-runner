class Util:
    @staticmethod
    def parse_for_key(url):
        key_start = url.find('/snowflake')
        key = url[key_start:]

        return key

    @staticmethod
    def get_view_from_stage(stage_name):
        if 'DAILY' in stage_name:
            return stage_name.replace('STG_DAILY', 'VW')
        elif 'INACTIVE' in stage_name:
            return stage_name.replace('STG_INACTIVE', 'VW')
        else:
            return stage_name.replace('STG', 'VW')