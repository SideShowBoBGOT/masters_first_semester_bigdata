SPARK_HOME := /usr/lib/spark
CP := "$(SPARK_HOME)/jars/*"
BUILD_DIR := $(CURDIR)/build

# ---- tweakable defaults ----
MASTER ?= local[*]
DRIVER_MEM ?= 1g
EXEC_MEM ?= 1g
JVM_OPTS ?= -XX:+UseG1GC -XX:+UseStringDeduplication

$(BUILD_DIR):
	mkdir -p $@

$(BUILD_DIR)/Main.class: Main.scala | $(BUILD_DIR)
	scalac -deprecation -feature -Xlint -cp $(CP) -d $(BUILD_DIR) $<

.PHONY: main
main: $(BUILD_DIR)/Main.class
	scala \
	  -J-Xms$(DRIVER_MEM) -J-Xmx$(DRIVER_MEM) $(addprefix -J, $(JVM_OPTS)) \
	  -J-Dspark.master=$(MASTER) \
	  -J-Dspark.driver.memory=$(DRIVER_MEM) \
	  -J-Dspark.executor.memory=$(EXEC_MEM) \
	  -cp "$(BUILD_DIR):$(SPARK_HOME)/jars/*" Main

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)

