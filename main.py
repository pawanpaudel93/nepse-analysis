from nepse import NEPSE


if __name__ == "__main__":
    nepse = NEPSE()
    print(nepse._get_floorsheet(symbol="GBBL", date="2021-08-11"))
    # nepse._get_sector_floorsheet(52, date="2021-08-11", top_n=5)
