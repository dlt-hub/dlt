import os
import stat
import pytest
import re
import pathvalidate
from pathlib import Path

from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import encoding_for_mode, set_working_dir, uniq_id

from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage, test_storage, skipifnotwindows


def test_storage_init(test_storage: FileStorage) -> None:
    # must be absolute path
    assert os.path.isabs(test_storage.storage_path)
    # may not contain file name (ends with / or \)
    assert os.path.basename(test_storage.storage_path) == TEST_STORAGE_ROOT

    # TODO: write more cases


def test_to_relative_path(test_storage: FileStorage) -> None:
    assert test_storage.to_relative_path(".") == "."
    assert test_storage.to_relative_path("") == ""
    assert test_storage.to_relative_path("a") == "a"
    assert test_storage.to_relative_path("a/b/c") == str(Path("a/b/c"))
    assert test_storage.to_relative_path("a/b/../..") == "."
    with pytest.raises(ValueError):
        test_storage.to_relative_path("a/b/../.././..")
    with pytest.raises(ValueError):
        test_storage.to_relative_path("../a/b/c")
    abs_path = os.path.join(test_storage.storage_path, "a/b/c")
    assert test_storage.to_relative_path(abs_path) == str(Path("a/b/c"))
    abs_path = os.path.join(test_storage.storage_path, "a/b/../c")
    assert test_storage.to_relative_path(abs_path) == str(Path("a/c"))


def test_make_full_path(test_storage: FileStorage) -> None:
    # fully within storage
    relative_path = os.path.join("dir", "to", "file")
    path = test_storage.make_full_path(relative_path)
    assert path.endswith(os.path.join(TEST_STORAGE_ROOT, relative_path))
    # overlapped with storage
    root_path = os.path.join(TEST_STORAGE_ROOT, relative_path)
    path = test_storage.make_full_path(root_path)
    assert path.endswith(root_path)
    assert path.count(TEST_STORAGE_ROOT) == 2
    # absolute path with different root than TEST_STORAGE_ROOT does not lead into storage so calculating full path impossible
    with pytest.raises(ValueError):
        test_storage.make_full_path(os.path.join("/", root_path))
    # relative path out of the root
    with pytest.raises(ValueError):
        test_storage.make_full_path("..")
    # absolute overlapping path
    path = test_storage.make_full_path(os.path.abspath(root_path))
    assert path.endswith(root_path)
    assert test_storage.make_full_path("") == test_storage.storage_path
    assert test_storage.make_full_path(".") == test_storage.storage_path


def test_in_storage(test_storage: FileStorage) -> None:
    # always relative to storage root
    assert test_storage.in_storage("a/b/c") is True
    assert test_storage.in_storage(f"../{TEST_STORAGE_ROOT}/b/c") is True
    assert test_storage.in_storage("../a/b/c") is False
    assert test_storage.in_storage("../../../a/b/c") is False
    assert test_storage.in_storage("/a") is False
    assert test_storage.in_storage(".") is True
    assert test_storage.in_storage(os.curdir) is True
    assert test_storage.in_storage(os.path.realpath(os.curdir)) is False
    assert test_storage.in_storage(os.path.join(os.path.realpath(os.curdir), TEST_STORAGE_ROOT)) is True


def test_from_wd_to_relative_path(test_storage: FileStorage) -> None:
    with pytest.raises(ValueError):
        test_storage.from_wd_to_relative_path(".")
    with pytest.raises(ValueError):
        test_storage.from_wd_to_relative_path("")
    with pytest.raises(ValueError):
        test_storage.from_wd_to_relative_path("chess.py")

    with set_working_dir(TEST_STORAGE_ROOT):
        assert test_storage.from_wd_to_relative_path(".") == "."
        assert test_storage.from_wd_to_relative_path("") == "."
        assert test_storage.from_wd_to_relative_path("a/b/c") == str(Path("a/b/c"))

    test_storage.create_folder("a")
    with set_working_dir(os.path.join(TEST_STORAGE_ROOT, "a")):
        assert test_storage.from_wd_to_relative_path(".") == "a"
        assert test_storage.from_wd_to_relative_path("") == "a"
        assert test_storage.from_wd_to_relative_path("a/b/c") == str(Path("a/a/b/c"))


def test_hard_links(test_storage: FileStorage) -> None:
    content = uniq_id()
    test_storage.save("file.txt", content)
    test_storage.link_hard("file.txt", "link.txt")
    # it is a file
    assert test_storage.has_file("link.txt")
    # should have same content as file
    assert test_storage.load("link.txt") == content
    # should be linked
    with test_storage.open_file("file.txt", mode="a") as f:
        f.write(content)
    assert test_storage.load("link.txt") == content * 2
    with test_storage.open_file("link.txt", mode="a") as f:
        f.write(content)
    assert test_storage.load("file.txt") == content * 3
    # delete original file
    test_storage.delete("file.txt")
    assert not test_storage.has_file("file.txt")
    assert test_storage.load("link.txt") == content * 3


def test_validate_file_name_component() -> None:
    # no dots
    with pytest.raises(ValueError):
        FileStorage.validate_file_name_component("a.b")
    # no slashes
    with pytest.raises(ValueError):
        FileStorage.validate_file_name_component("a/b")
    # no backslashes
    with pytest.raises(ValueError):
        FileStorage.validate_file_name_component("a\\b")

    FileStorage.validate_file_name_component("BAN__ANA is allowed")


@skipifnotwindows
def test_rmtree_ro(test_storage: FileStorage) -> None:
    test_storage.create_folder("protected")
    path = test_storage.save("protected/barbapapa.txt", "barbapapa")
    os.chmod(path, stat.S_IREAD)
    os.chmod(test_storage.make_full_path("protected"), stat.S_IREAD)
    with pytest.raises(PermissionError):
        test_storage.delete_folder("protected", recursively=True, delete_ro=False)
    test_storage.delete_folder("protected", recursively=True, delete_ro=True)
    assert not test_storage.has_folder("protected")


def test_encoding_for_mode() -> None:
    assert encoding_for_mode("b") is None
    assert encoding_for_mode("bw") is None
    assert encoding_for_mode("t") == "utf-8"
    assert encoding_for_mode("a") == "utf-8"
    assert encoding_for_mode("w") == "utf-8"


def test_save_atomic_encode() -> None:
    tstr = "data'ऄअआइ''ईउऊऋऌऍऎए');"
    FileStorage.save_atomic(TEST_STORAGE_ROOT, "file.txt", tstr)
    storage = FileStorage(TEST_STORAGE_ROOT)
    with storage.open_file("file.txt") as f:
        assert f.encoding == "utf-8"
        assert f.read() == tstr

    bstr = b"axa\0x0\0x0"
    FileStorage.save_atomic(TEST_STORAGE_ROOT, "file.bin", bstr, file_type="b")
    storage = FileStorage(TEST_STORAGE_ROOT, file_type="b")
    with storage.open_file("file.bin", mode="r") as f:
        assert hasattr(f, "encoding") is False
        assert f.read() == bstr


####### BEGIN AUTOGENERATED TESTS #######

@pytest.fixture
def file_storage():
    return FileStorage('/tmp/test', makedirs=True)

@pytest.fixture
def test_file_path():
    test_file_path = '/tmp/test/test.txt'
    with open(test_file_path, 'w') as f:
        f.write('test')
    return test_file_path

def test_init(file_storage):
    assert file_storage.storage_path == os.path.realpath('/tmp/test')
    assert file_storage.file_type == 't'
    assert os.path.exists(file_storage.storage_path)





@pytest.fixture
def file_storage():
    return FileStorage('test_storage', 't')

@pytest.mark.parametrize('relative_path, recursively, delete_ro', [('test_folder', True, False), ('test_folder', True, True)])
def test_delete_folder(file_storage, relative_path, recursively, delete_ro):
    folder_path = file_storage.make_full_path(relative_path)
    os.makedirs(folder_path, exist_ok=True)
    file_storage.delete_folder(relative_path, recursively, delete_ro)
    assert not os.path.exists(folder_path)

def test_delete_folder_not_directory(file_storage):
    with pytest.raises(NotADirectoryError):
        file_storage.delete_folder('test_file.txt', False, False)




@pytest.fixture
def file_storage():
    return FileStorage('test_storage', makedirs=True)

@pytest.mark.parametrize('relative_path, mode, expected_encoding', [('test_file', 'r', 'utf-8'), ('test_file', 'w', 'utf-8'), ('test_file', 'r+', 'utf-8'), ('test_file', 'w+', 'utf-8')])
def test_open_file(file_storage, relative_path, mode, expected_encoding):
    f = file_storage.open_file(relative_path, mode)
    assert f.mode == mode + file_storage.file_type
    assert f.encoding == expected_encoding

def test_open_file_raises_exception(file_storage):
    with pytest.raises(IOError):
        file_storage.open_file('invalid_path', 'r')




@pytest.fixture
def file_storage():
    return FileStorage('/tmp/test_storage', 't', makedirs=True)

def test_has_file_exists(file_storage):
    """ Test that has_file returns true when the file exists """
    test_file = os.path.join(file_storage.storage_path, 'test.txt')
    with open(test_file, 'w') as f:
        f.write('test')
    assert file_storage.has_file('test.txt') is True
    os.remove(test_file)

def test_has_file_not_exists(file_storage):
    """ Test that has_file returns false when the file does not exist """
    assert file_storage.has_file('test.txt') is False



@pytest.fixture
def file_storage():
    return FileStorage('/tmp', 't', makedirs=True)

def test_create_folder_exists_ok_true(file_storage):
    file_storage.create_folder('test_folder', exists_ok=True)
    assert os.path.exists(file_storage.make_full_path('test_folder'))

def test_create_folder_exists_ok_false(file_storage):
    with pytest.raises(OSError):
        file_storage.create_folder('test_folder', exists_ok=False)

def test_create_folder_relative_path(file_storage):
    file_storage.create_folder('test_folder/sub_folder', exists_ok=True)
    assert os.path.exists(file_storage.make_full_path('test_folder/sub_folder'))

def test_create_folder_absolute_path(file_storage):
    with pytest.raises(ValueError):
        file_storage.create_folder('/test_folder', exists_ok=True)





@pytest.fixture
def file_storage(tmp_path):
    storage_path = tmp_path / 'storage'
    os.makedirs(storage_path)
    yield FileStorage(storage_path)

def test_atomic_rename_with_valid_inputs(file_storage):
    from_relative_path = 'from.txt'
    to_relative_path = 'to.txt'
    from_full_path = file_storage.make_full_path(from_relative_path)
    to_full_path = file_storage.make_full_path(to_relative_path)
    open(from_full_path, 'w').close()
    file_storage.atomic_rename(from_relative_path, to_relative_path)
    assert os.path.exists(to_full_path)
    assert not os.path.exists(from_full_path)




@pytest.fixture
def file_storage():
    storage_path = os.path.realpath('test_data')
    file_type = 't'
    makedirs = False
    return FileStorage(storage_path, file_type, makedirs)

def test_in_storage_with_relative_path(file_storage):
    path = 'test_file.txt'
    assert file_storage.in_storage(path) == True

def test_in_storage_with_invalid_path(file_storage):
    path = os.path.realpath('invalid_path/test_file.txt')
    assert file_storage.in_storage(path) == False




@pytest.fixture
def file_storage():
    return FileStorage('/tmp', 't', True)

@pytest.mark.parametrize('file_path, expected', [('/Users/test/myfile.txt', 'myfile.txt'), ('/Users/test/myfile.py', 'myfile.py'), ('/Users/test/myfile', 'myfile')])
def test_get_file_name_from_file_path(file_storage, file_path, expected):
    assert file_storage.get_file_name_from_file_path(file_path) == expected






FILE_COMPONENT_INVALID_CHARACTERS = re.compile('[.%{}]')

@pytest.fixture
def storage_path():
    return './test/'

@pytest.fixture
def file_type():
    return 't'

@pytest.fixture
def makedirs():
    return True

@pytest.fixture
def file_storage(storage_path, file_type, makedirs):
    return FileStorage(storage_path, file_type, makedirs)

@pytest.mark.parametrize('name, expected_exception', [('name', None), ('name%txt', pathvalidate.error.InvalidCharError), ('name{txt', pathvalidate.error.InvalidCharError), ('name}txt', pathvalidate.error.InvalidCharError)])
def test_validate_file_name_component(name, expected_exception):
    if expected_exception:
        with pytest.raises(expected_exception):
            FileStorage.validate_file_name_component(name)
    else:
        FileStorage.validate_file_name_component(name)






@pytest.fixture
def file_storage():
    return FileStorage('/tmp/storage', 't', True)

def test_init(file_storage):
    assert os.path.exists('/tmp/storage')

def test_rmtree_del_ro(file_storage):
    file_name = os.path.join(file_storage.storage_path, 'test_file.txt')
    with open(file_name, 'w') as f:
        f.write('This is a test file')
    os.chmod(file_name, stat.S_IREAD)
    file_storage.rmtree_del_ro(os.unlink, file_name, None)
    assert not os.path.exists(file_name)