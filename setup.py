from setuptools import setup, find_packages

setup(name="anonymize", 
      author = "Michal Zylinski",
      author_email = "michal.zylinski@gmail.com",
      license = "MIT",
      url="https://github.com/MichalZylinski/anonymize",
      version="0.6", 
      description="Simple yet efficient data anonymization framework",
      packages=['anonymize','examples'],
      package_data={'':['*.json', '*.csv']},
      py_modules=['anonymize_script'],
      entry_points={'console_scripts':['anonymize=anonymize_script:main']},
      classifiers=[
          "Programming Language :: Python :: 2.7",
          "Programming Language :: Python :: 3"
          ],      
      install_requires=["pika"]
      )




