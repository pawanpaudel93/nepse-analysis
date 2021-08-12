from nepse import NEPSE


if __name__ == "__main__":
    nepse = NEPSE()
    nepse.display_floorsheet(symbol="HIDCL")
    nepse.display_sector_floorsheet(52, top_n=5)
