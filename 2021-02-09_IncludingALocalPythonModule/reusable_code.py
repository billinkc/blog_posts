# reusable_code.py is a python module that simulates our desire to 
#consolidate our corporate business logic into a re-usable entity

from datetime import datetime

class Configuration():
    """A very important class that provides a standardized approach for our company"""
    def __init__(self):
        # The modify date drives very important business functionality
        # so let's be consitent in how it is defined (ISO 8601)
        # 2021-02-07T17:58:20Z
        self.__modify_date__ = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    def get_modify_date(self):
        """Provide a standard interface for accessing the modify date"""
        return self.__modify_date__


def main():
    c = Configuration()
    print(c.get_modify_date())

if __name__ == "__main__":
    main()