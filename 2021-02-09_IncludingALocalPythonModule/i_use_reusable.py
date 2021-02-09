# This module lives in the same folder as our reusable_code.py file
import reusable_code


def main():
    c = reusable_code.Configuration()
    print(c.get_modify_date())


if __name__ == '__main__':
    main()