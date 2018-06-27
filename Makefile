all: build

PROJECT_NAME=LeanIPC
DEPENDS_CS=$(shell find . -type f -name *.cs | xargs echo)
VERSION=$(shell cat $(PROJECT_NAME).nuspec | grep "<version>" | cut -f 2 -d ">" | cut -f 1 -d "<" )

build: $(PROJECT_NAME).sln $(DEPENDS_CS)
	nuget restore $(PROJECT_NAME).sln
	# dotnet restore Unittest
	# dotnet build /p:Configuration=Release Unittest
	msbuild /p:Configuration=Release $(PROJECT_NAME).sln

nupkg/$(PROJECT_NAME).$(VERSION).nupkg: $(PROJECT_NAME).nuspec
	nuget pack $(PROJECT_NAME).nuspec

pack: build nupkg/$(PROJECT_NAME).$(VERSION).nupkg

deploy: pack
	nuget push $(PROJECT_NAME).$(VERSION).nupkg -source "https://www.nuget.org"

clean:
	msbuild /t:Clean $(PROJECT_NAME).sln
	find . -type d -name obj | xargs rm -r
	find . -type d -name bin | xargs rm -r


.PHONY: all build clean deploy pack
