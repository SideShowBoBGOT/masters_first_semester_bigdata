.PHONY: all

TRAIN_DATA_DIR:=$(CURDIR)/traindata
ZIP_ARCHIVE:=$(TRAIN_DATA_DIR)/individual+household+electric+power+consumption.zip
UNPACKED_DATA:=$(TRAIN_DATA_DIR)/household_power_consumption.txt

all: $(UNPACKED_DATA)

$(TRAIN_DATA_DIR):
	mkdir -p $@

$(ZIP_ARCHIVE): | $(TRAIN_DATA_DIR)
	wget http://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip -O $@

$(UNPACKED_DATA): | $(ZIP_ARCHIVE)
	unzip $(ZIP_ARCHIVE) -d $(TRAIN_DATA_DIR)

.PHONY: clean

clean:
	rm -rf $(TRAIN_DATA_DIR)
