DESCRIPTION_URL:=https://www.kaggle.com/datasets/zhaxidelebsz/10-gym-exercises-with-615-abstracted-features/data
TRAIN_DATA_DIR:=$(CURDIR)/traindata
ARCHIVE:=$(TRAIN_DATA_DIR)/data.zip
UNPACKED_DATA:=$(TRAIN_DATA_DIR)/RecGym.csv
URL:=https://www.kaggle.com/api/v1/datasets/download/zhaxidelebsz/10-gym-exercises-with-615-abstracted-features

.PHONY: all
all: $(UNPACKED_DATA)

$(TRAIN_DATA_DIR):
	mkdir -p $@

$(ARCHIVE): | $(TRAIN_DATA_DIR)
	curl -L -o $@ $(URL)

$(UNPACKED_DATA): | $(ARCHIVE)
	unzip $(ARCHIVE) -d $(TRAIN_DATA_DIR)

.PHONY: clean
clean:
	rm -rf $(TRAIN_DATA_DIR)
