import os, sys, unittest, shutil, tempfile

my_folder = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
dis_path = os.path.join(my_folder, '..')
# Since the script we're testing doesn't have a .py extension, we can't
# import it directly. Make a copy that we can work with.
shutil.copy(os.path.join(dis_path, 'dis'), os.path.join(my_folder, 'testable_dis.py'))

from testable_dis import *

class dis_test(unittest.TestCase):
    
    def test_json_round_trip_with_source_item(self):
        x = source_item(__file__)
        json_txt = json.dumps(x, cls=custom_encoder)
        self.assertTrue(json_txt.find('source_item') == -1)
        self.assertTrue(json_txt.find('lastmod_time') > -1)
        j = json.loads(json_txt)
        y = source_item(j)
        
    def test_load_job1(self):
        j = job.load(os.path.join(my_folder, 'job1'))
        self.assertEquals('dec2a3445bd411e5a4b390b11c92dea7', j.uuid)
        self.assertEquals([], j.pending)
        self.assertEquals(1442340553, int(j.queue_time))
        self.assertEquals(1, len(j.sources))
        self.assertEquals(1442258491, j.sources[0].lastmod_time)
        # Prove we can write a property and read it back
        j.uuid = 'x'
        self.assertEquals('x', j.uuid)
        
    def test_load_job2(self):
        j = job.load(os.path.join(my_folder, 'job2'))
        self.assertEquals('91f3bd3a5c2611e5a18c90b11c92dea7', j.uuid)
        self.assertEquals(2, len(j.pending))
        self.assertEquals(1442375643, int(j.queue_time))
        self.assertEquals(1, len(j.sources))
        self.assertEquals(1441063112, j.sources[0].lastmod_time)

    def test_json_round_trip_with_job(self):
        j = job.load(os.path.join(my_folder, 'job1'))
        fd, path = tempfile.mkstemp()
        os.close(fd)
        try:
            j.save(path)
            self.assertEquals(path, j.get_path())
            self.assertFalse(j.is_busy())
            k = job.load(path)
            jraw = j.raw
            kraw = k.raw
            self.assertEquals(len(jraw), len(kraw))
            for key in jraw:
                self.assertEquals(jraw[key], kraw[key])
        finally:
            os.remove(path)
            
    def test_job_upgrade(self):
        fd, path = tempfile.mkstemp()
        os.write(fd, '{}')
        os.close(fd)
        try:
            j = job.load(path)
            for key in job.prototype.raw.keys():
                self.assertTrue(key in j.raw.keys())
        finally:
            os.remove(path)
            
    def test_job_empty(self):
        j = job.empty()
        self.assertTrue(j.uuid)
        self.assertTrue(is_string(j.uuid))
        self.assertTrue(j.get_path() is None)
            
    def test_json_wrapper_has_normal_dict_methods(self):
        a = {'a': 1, 'b': 2}
        x = json_wrapper(a)
        x.keys()


if __name__ == '__main__':
    unittest.main()