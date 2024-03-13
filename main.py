import datetime
import os
import pandas
import asyncio
from aiopath import AsyncPath
from genericpath import exists
from PIL import Image


class TextDict:
    columns_list = ["day", "month", "year", "pages", "s.pages", "pages"]
    result_columns_list = ["date", "scanner", "chartref.date", "scannedby", "issuename", "year",
                           "inventorypagecount", "scannedpagescount", "startflag", "bookname"]
    metadate_column = "Book name"
    clean_value = "Sum 1"
    clean_pages = "pages"
    start_value = "start"
    stop_value = "stop"
    result_sheet = "3"
    scan_num = ""
    sheet_key = "sheet_data"
    metadate_key = ""


class ExelTable:
    exel_packet: pandas = pandas
    xl = None

    def read_file(self, file_path):
        self.xl = self.exel_packet.ExcelFile(file_path)


class Sheets(ExelTable, TextDict):
    sheets = {}

    def get_sheets(self, valid_columns):

        for sht, clm in valid_columns.items():

            sheet_data = self.xl.parse(sht)[clm[:-1]].fillna(0)

            try:
                book_metadate = clm[-1]
            except IndexError:
                book_metadate = self.metadate_column

            self.sheets[sht] = {self.sheet_key: sheet_data, self.metadate_key: book_metadate}

    def validate_sheets(self):
        for key, value in self.sheets.items():
            df = value[self.sheet_key]
            rename_columns = dict(zip(df.columns, self.columns_list))
            df.rename(columns=rename_columns, inplace=True)
            index_clean = df[df[self.clean_pages] == self.clean_value].index[0]
            df.drop(labels=range(index_clean, len(df)), axis=0, inplace=True)


class Columns(ExelTable, TextDict):
    columns: dict[str:str] = {}
    valid_columns: dict[str:str] = {}

    def get_columns(self):
        sheet_xl = self.xl.sheet_names

        for sheet in sheet_xl:
            try:
                int(sheet)
                self.columns[sheet] = list(self.xl.parse(sheet).columns)

            except ValueError:
                pass

    def validate_columns(self, columns_list):

        for sheet, column in self.columns.items():
            valid_columns = list(filter(lambda x: x.replace(" ", "").lower() in columns_list or column.index(x) == (len(column) - 1), [clm for clm in column]))
            self.valid_columns[sheet] = valid_columns

    def columns_read(self):
        self.day, self.month, self.year, self.pages, self.s_pages, self.metadate = self.valid_columns[
            self.result_sheet]  # need to update

    def columns_result(self):
        self.date, self.scanner_num, self.utt_name, self.user_name, self.issue_name, self.issue_year, self.inv_pages, \
            self.scan_pages, self.start_flag, self.book_name = self.valid_columns[self.result_sheet]


class ExelFile(Sheets, Columns):

    def __init__(self, file_path):
        self.valid_columns = {}
        self.columns = {}
        self.sheets = {}
        self.file_path = file_path
        self.xl = None

    def get_book_xl(self):
        self.read_file(self.file_path)
        self.get_columns()
        self.validate_columns(self.columns_list)
        self.columns_read()
        self.get_sheets(self.valid_columns)
        self.validate_sheets()

    def get_result_xl(self):
        self.read_file(self.file_path)
        self.get_columns()
        self.validate_columns(self.result_columns_list)
        self.get_sheets(self.valid_columns)
        self.columns_result()

    def write_result(self):
        start_row = 0
        sheet_exists = "new"
        sheet = {}

        if self.result_sheet in self.sheets:
            sheet_exists = "overlay"
            start_row = len(self.sheets[self.result_sheet][self.sheet_key])

        with self.exel_packet.ExcelWriter(self.file_path, engine="openpyxl", mode="a", if_sheet_exists=sheet_exists) \
                as result_file:
            df1 = self.exel_packet.DataFrame(sheet)
            df1.to_excel(result_file, sheet_name=self.result_sheet, startrow=start_row)


class CreateXl(TextDict):

    def __init__(self, user_sheets, sheets: ExelFile):
        self.month = None
        self.year = None
        self.day = None
        self.today = datetime.datetime.now().date().strftime("%d/%m/%Y")
        self.sheet = None
        self.user_sheets = user_sheets
        self.sheets = sheets
        self.sheet_list = []
        self.frame_dict = {}

        self.count = "01"
        self.count_pages = 0
        self.date = 0

    def create_result(self):
        for sh_name in self.sheet_list:
            self.sheet = self.sheets.sheets[sh_name][self.sheet_key]

            self.frame_dict[sh_name] = {"date": [], "year": [], "issue": [], "inv.pages": [], "book_name": []}

            i = 0
            while i < len(self.sheet):
                self.validate_date(sh_name, i)
                self.validate_pages_count(i)
                self.validate_today()

                self.frame_dict[sh_name]["issue"].append(self.date)
                self.frame_dict[sh_name]["inv.pages"].append(self.count_pages)
                self.frame_dict[sh_name]["date"].append(self.today)
                self.frame_dict[sh_name]["year"].append(self.year)
                self.frame_dict[sh_name]["book_name"].append(self.sheets.sheets[sh_name][self.metadate_key])
                i += 1

    def validate_user_sheets(self):
        self.sheet_list = [sheet.strip() for sheet in self.user_sheets if len(sheet.strip()) > 0]

    def validate_date(self, sh_name, i):
        self.day = str(int(self.sheet[self.sheets.day.lower()][i]))
        self.month = str(int(self.sheet[self.sheets.month.lower()][i]))
        self.year = str(int(self.sheet[self.sheets.year.lower()][i]))

        if len(self.day) == 1:
            self.day = "0" + self.day
        if len(self.month) == 1:
            self.month = "0" + self.month
        if len(self.year) == 1:
            self.year = "0" + self.year

        self.date = f"{self.year}-{self.month}-{self.day}_{self.count}"
        count_while = 1

        while self.date in self.frame_dict[sh_name]["issue"]:
            count = self.count

            if count_while < 10:
                count = "0" + str(count_while)

            self.date = f"{self.year}-{self.month}-{self.day}_{count}"

            count_while += 1

    def validate_pages_count(self, i):
        page = self.sheet[self.sheets.pages.lower()][i]
        s_page = self.sheet[self.sheets.s_pages.lower().replace(" ", "")][i]
        self.count_pages = page + s_page

    def validate_today(self):
        # current_date = datetime.datetime.now().date()
        # excel_date = float((current_date - datetime.datetime(1899, 12, 30).date()).days)
        self.today = datetime.datetime.now().date().strftime("%d/%m/%Y")
        # self.today = excel_date


class DataframeExel:

    def __init__(self, result_xl: ExelFile, write_xl: CreateXl, path_result):
        self.issue = None
        self.inv_pages = None
        self.year = None
        self.sheet = None
        self.date = None
        self.start_row = None
        self.start_col_date = 0
        self.start_col_year = 0
        self.start_col_issue = 0
        self.start_col_inv_pages = 0
        self.result_xl = result_xl
        self.sheet_key = self.result_xl.sheet_key
        self.result_sheet = self.result_xl.result_sheet
        self.write_xl = write_xl
        self.path_result = path_result

    def create_dataframe(self):
        for sheet in self.write_xl.frame_dict:
            self.sheet = self.write_xl.frame_dict[sheet]
            with self.result_xl.exel_packet.ExcelWriter(self.result_xl.file_path, engine="openpyxl", mode="a",
                                                        if_sheet_exists="overlay") as result_file:
                self.create_data(result_file)
                self.create_year(result_file)
                self.create_issue(result_file)
                self.create_inv_pages(result_file)

    def create_data(self, result_file):
        if self.start_col_date == 0:
            self.start_col_date = self.result_xl.xl.parse(self.result_xl.result_sheet).columns.get_loc(
                self.result_xl.date)

        self.start_row = len(self.result_xl.sheets[self.result_sheet][self.sheet_key][self.result_xl.date]) + 2
        self.date = self.result_xl.exel_packet.Series(self.sheet["date"], name=self.result_xl.date)

        self.date.to_excel(result_file, sheet_name=self.result_xl.result_sheet, index=False,
                           startrow=self.start_row, startcol=self.start_col_date, header=False)

    def create_year(self, result_file):
        if self.start_col_year == 0:
            self.start_col_year = self.result_xl.xl.parse(self.result_xl.result_sheet).columns.get_loc(
                self.result_xl.issue_year)
        # self.start_row = len(self.result_xl.sheets[self.result_sheet][self.sheet_key][self.result_xl.issue_year]) + 2
        self.year = self.result_xl.exel_packet.Series(self.sheet["year"], name="year")
        self.year.to_excel(result_file, sheet_name=self.result_xl.result_sheet, index=False,
                           startrow=self.start_row, startcol=self.start_col_year, header=False)

    def create_issue(self, result_file):
        if self.start_col_issue == 0:
            self.start_col_issue = self.result_xl.xl.parse(self.result_xl.result_sheet).columns.get_loc(
                self.result_xl.issue_name)
        # self.start_row = len(self.result_xl.sheets[self.result_sheet][self.sheet_key][self.result_xl.issue_name]) + 2
        self.issue = self.result_xl.exel_packet.Series(self.sheet["issue"], name="issue")
        self.issue.to_excel(result_file, sheet_name=self.result_xl.result_sheet, index=False,
                            startrow=self.start_row, startcol=self.start_col_issue, header=False)

    def create_inv_pages(self, result_file):
        if self.start_col_inv_pages == 0:
            self.start_col_inv_pages = self.result_xl.xl.parse(self.result_xl.result_sheet).columns.get_loc(
                self.result_xl.inv_pages)
        # self.start_row = len(self.result_xl.sheets[self.result_sheet][self.sheet_key][self.result_xl.inv_pages]) + 2
        self.inv_pages = self.result_xl.exel_packet.Series(self.sheet["inv.pages"], name="inv.pages")
        self.inv_pages.to_excel(result_file, sheet_name=self.result_xl.result_sheet, index=False,
                                startrow=self.start_row, startcol=self.start_col_inv_pages, header=False)


class ScanFolder:
    start_ind = 0
    end_ind = 0
    img_list = []

    # need to update for exist file and extensions

    def __init__(self, fld_path: str, result_xl: ExelFile):
        self.exs = None
        self.img_name = None
        self.check = True
        self.folder_dict = None
        self.fld_path = fld_path
        self.result_xl = result_xl
        self.sheet = self.result_xl.sheets[self.result_xl.result_sheet][self.result_xl.sheet_key]

    def get_index(self):
        self.start_ind = self.sheet[self.sheet[self.result_xl.start_flag] == self.result_xl.start_value].index[0]

        self.end_ind = self.sheet[self.sheet[self.result_xl.start_flag] == self.result_xl.stop_value].index[0] + 1

    def folder_pages_dict(self):
        self.folder_dict = {}
        zip_list = zip(self.sheet[self.result_xl.issue_name][self.start_ind:self.end_ind],
                                    self.sheet[self.result_xl.inv_pages][self.start_ind:self.end_ind])

        for folder_name, page_count in zip_list:

            if folder_name in self.folder_dict.keys():
                raise Exception(f"Duplicate folder name {folder_name}")
            else:
                self.folder_dict[folder_name] = page_count

        for fld_name, count_page in self.folder_dict.items():
            img_slice = self.img_list[0:int(count_page)]
            self.folder_dict[fld_name] = img_slice
            del self.img_list[0:int(count_page)]

    def check_folder(self):
        with os.scandir(self.fld_path) as files:
            self.img_list = [file.name for file in files if file.is_file() and file.name.endswith(".tif")]

        if len(self.img_list) != sum(self.sheet[self.result_xl.inv_pages][self.start_ind:self.end_ind]):
            self.check = False

    def create_name(self, i, img_name):
        start_name = "0000"
        new_name = start_name + str(i)
        self.img_name = new_name[-4:]
        self.exs = "." + img_name.split(".")[-1]

    def folders_create(self):
        for folder, value in self.folder_dict.items():

            if not exists(f"{self.fld_path}\\{folder}") and folder != 0 and value != []:
                os.mkdir(f"{self.fld_path}\\{folder}")

    async def replace_img(self):
        for fld_name, img_name in self.folder_dict.items():

            for i in range(1, len(img_name) + 1):
                self.create_name(i, img_name[0])
                folder = AsyncPath(f"{self.fld_path}\\{img_name[0]}")
                del img_name[0]
                await folder.replace(f"{self.fld_path}\\{fld_name}\\{self.img_name}{self.exs}")


class ReturnSort:
    def __init__(self, fld_path):
        self.fld_path = fld_path
        self.file_dict = {}

    def create_name(self, i, img):
        start_name = "0000"
        new_name = start_name + str(i)
        img_name = new_name[-4:]
        exs = "." + img.split(".")[-1]
        self.name = img_name + exs

    def get_filedict(self):
        # with os.walk(self.fld_path) as files:
        i = 0
        for root, folder, file in os.walk(self.fld_path):
            names = []
            if root != self.fld_path:
                for fl in file:

                    i += 1
                    self.create_name(i, fl)
                    names.append(self.name)

                self.file_dict[root] = {"files": file, "names": names}

    async def replace_img(self):

        for path, img_dict in self.file_dict.items():
            for _ in range(1, len(img_dict['files']) + 1):
                folder = AsyncPath(f"{path}\\{img_dict['files'][0]}")
                await folder.replace(f"{self.fld_path}\\{img_dict['names'][0]}")
                del img_dict['files'][0]
                del img_dict['names'][0]
            os.rmdir(path)


class ChangeMetadate:
    def __init__(self, fld_path, metadate):
        self.fld_path = fld_path
        self.metadate = metadate
        self.img_list = []

    def get_filelist(self):
        with os.scandir(self.fld_path) as files:
            self.img_list = [file.name for file in files if file.is_file() and file.name.endswith(".tif")]

    def change_metadate(self):
        for img in self.img_list:
            img_path = self.fld_path + "\\" + img
            with Image.open(img_path) as test_image:
                test_image.tag[269] = self.metadate

                test_image.save(img_path, tiffinfo=test_image.tag)

    def start_script(self):
        self.get_filelist()
        self.change_metadate()


class CleanReportFile():

    def __init__(self, exel_path):
        self.exel_path = exel_path.strip()
        self.result_xl = ExelFile(self.exel_path)
        self.result_xl.get_result_xl()
        self.sheet = self.result_xl.sheets[self.result_xl.result_sheet][self.result_xl.sheet_key]

    def get_index(self):
        self.start_ind = self.sheet[self.sheet[self.result_xl.start_flag] == self.result_xl.start_value].index[0]
        self.end_ind = self.sheet[self.sheet[self.result_xl.start_flag] == self.result_xl.stop_value].index[0] + 1

    def take_sheets(self):
        self.work_sheets = self.sheet[self.start_ind: self.end_ind]

        self.del_index = list((self.work_sheets[(self.work_sheets[self.result_xl.inv_pages] == 0)]).index)

    def clean_sheets(self):
        sheet = self.result_xl.xl.parse(sheet_name="3")
        self.sheet = sheet.drop(index=self.del_index, axis=0)
        # end_index = len(self.sheet)
        # self.sheet = sheet.drop(index=list(range(end_index, self.end_ind)), axis=0)

    def change_xl(self):
        with self.result_xl.exel_packet.ExcelWriter(self.result_xl.file_path, engine="openpyxl", mode="a",
                                                    if_sheet_exists="overlay") as result_file:

            self.sheet.to_excel(result_file, sheet_name=self.result_xl.result_sheet, index=False,
                                    startrow=0, startcol=0, header=True)


def start_sort():
    while True:
        write_file = input("Write result file path or 0 enter to menu : >>> ").strip()
        if write_file == "0":
            return
        fl = input("Write scan folder with image: >>> ").strip()

        user_inp = input(f"All is good?\n File path: {write_file}, \n Folder path: {fl} "
                         f"\n Write 1 or yes for start sorting \n>>> ").strip().lower()

        if user_inp == "1" or user_inp == "yes":

            try:
                xl_life = ExelFile(write_file)
                xl_life.get_result_xl()
                scan_fld = ScanFolder(fl, xl_life)
                scan_fld.get_index()
                scan_fld.check_folder()

                if not scan_fld.check:
                    user_inp = input(f"You scan {len(scan_fld.img_list)}, but inventory count is "
                                     f"{sum(scan_fld.sheet[scan_fld.result_xl.inv_pages][scan_fld.start_ind:scan_fld.end_ind])}"
                                     f"\nCheck scan folder and inventory file! \nWrite 1 or yes to continue,  "
                                     f"3 or no to return >>> ").strip().lower()

                if user_inp == "1" or user_inp == "yes":
                    xl_life = ExelFile(write_file)
                    xl_life.get_result_xl()
                    scan_fld = ScanFolder(fl, xl_life)
                    scan_fld.get_index()
                    scan_fld.check_folder()

                    scan_fld.folder_pages_dict()
                    scan_fld.folders_create()
                    asyncio.run(scan_fld.replace_img())
                    xl_life.xl.close()
                    print("Image sorting finished")

            except Exception as exep:
                print("Something is wrong!")
                print(exep)


def write_bookdate():
    import warnings
    warnings.filterwarnings("ignore", category=UserWarning)

    while True:
        user_inp = input("Write path to result file or 0 enter to menu: >>> ").strip()
        if user_inp == "0":
            return
        read_path = input("Write path to inventory book file: >>> ").strip()
        user_sheets = input("Write sheets name in inventory book: >>> ").split(",")
        user_check = input(
            f"All is good?\n Path to result file: {user_inp}, \n Path to inventory book file: {read_path} "
            f"Sheets names: {user_sheets}\n"
            f"\n Write 1 or yes for start write in result file\n>>> ").strip().lower()

        if user_check == "1" or user_check == "yes":
            read_xl = ExelFile(read_path)
            try:

                read_xl.result_sheet = user_sheets[0].strip()
                read_xl.get_book_xl()

                create_xl = CreateXl(user_sheets, read_xl)
                create_xl.validate_user_sheets()
                create_xl.create_result()

                result_file = ExelFile(user_inp)
                result_file.get_result_xl()

                write_file = DataframeExel(result_file, create_xl, user_inp)
                write_file.create_dataframe()
                print("Finishing writing book dates in file!")
                write_file.result_xl.xl.close()

            except Exception as exep:
                read_xl.xl.close()
                print("Something is wrong!")
                print(exep)


def return_sort():
    while True:
        user_inp = input("Write path to folder or 0 enter to menu: >>> ").strip()
        if user_inp == "0":
            return

        ret_sort = ReturnSort(user_inp)
        ret_sort.get_filedict()
        asyncio.run(ret_sort.replace_img())
        print("Finishing replacing images!")


def change_metadate():
    while True:
        user_inp = input("Write path to folder or 0 enter to menu: >>> ").strip()
        if user_inp == "0":
            return
        user_metadate = input("Write new name: >>> ").strip()
        md = ChangeMetadate(user_inp, user_metadate)
        md.start_script()
        print("Finishing!")


def clean_result_file():
    while True:
        user_inp = input("Write path to result file or 0 enter to menu: >>> ").strip()
        if user_inp == "0":
            return

        clean_file = CleanReportFile(user_inp)
        clean_file.get_index()
        clean_file.take_sheets()
        clean_file.clean_sheets()
        clean_file.change_xl()
        clean_file.result_xl.xl.close()

        print("Finishing!")


FUNC_DICT = {"1": start_sort,
             "2": write_bookdate,
             "3": return_sort,
             "4": clean_result_file,
             "*1*": change_metadate}

if __name__ == "__main__":

    while True:

        user_input = input("Chose program:\n"
                           "1 - Sorting files\n"
                           "2 - Write book date in result file\n"
                           "3 - Return sorting\n"
                           "4 - Clean result file\n>>> ").strip().lower()
        try:
            func = FUNC_DICT[user_input]
            func_start = func()
        except Exception as ex:
            print(ex)


