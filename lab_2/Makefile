SPARK_HOME := /usr/lib/spark
CP := "$(SPARK_HOME)/jars/*"
BUILD_DIR := $(CURDIR)/build

$(BUILD_DIR):
	mkdir -p $@

$(BUILD_DIR)/Main.class: Main.scala | $(BUILD_DIR)
	scalac -deprecation -feature -Xlint -cp $(CP) -d $(BUILD_DIR) $<

.PHONY: main
main: $(BUILD_DIR)/Main.class
	scala -cp "$(BUILD_DIR):$(SPARK_HOME)/jars/*" Main

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)

