# -*- coding: utf-8 -*-
import sqlalchemy as sa
from sqlalchemy import UniqueConstraint
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
import MySQLdb
import sqlalchemy
from sqlalchemy import update
import sys
from sqlalchemy import and_


class YelpEkzamPipeline(object):
    def __init__(self):
        self.engine = sa.create_engine('mysql+mysqldb://root:14061987@localhost/mysql')
        self.engine.echo = True
        metadata = sa.MetaData(self.engine)

        self.data = sa.Table('yelp_tab', metadata,
            sa.Column('id', Integer, primary_key=True),
            sa.Column('Title', sa.String(300)),
            sa.Column('Address', sa.String(500)),
            sa.Column('Phone', sa.String(100)),
            sa.Column('Email', sa.String(100)),
            sa.Column('Web', sa.String(100)),
            sa.Column('Schedule', sa.String(500)),
            sa.Column('About', sa.Text(15000)),
            sa.Column('Image', sa.String(300)),
            sa.Column('Reviews_number', Integer),
            UniqueConstraint('Title', 'Address', name='uix_1')
        )
        metadata.create_all()

    def process_item(self, item, spider):
        try:
            i = self.data.insert()
            i.execute(
                {
                    'Title': item.get('Title'),
                    'Address': item.get('Address'),
                    'Phone': item.get('Phone'),
                    'Email': item.get('Email'),
                    'Web': item.get('Web'),
                    'Schedule': item.get('Schedule'),
                    'About': item.get('About'),
                    'Image': item.get('Image'),
                    'Reviews_number': item.get('Reviews_number'),
                }
                )
        except sqlalchemy.exc.IntegrityError as e:
            # EXISTING RECORD IS UPDATING, NOT SKIP, NOT DUPLICATED
            print( "### EXISTING RECORD -> UPDATING ###\n%s" % e)

            upd = self.data.update().\
                        where(
                            and_(
                                self.data.c.Title == str(item.get('Title')),
                                self.data.c.Address == str(item.get('Address'))
                            )).\
                        values(
                            Title = item.get('Title'),
                            Address = item.get('Address'),
                            Phone = item.get('Phone'),
                            Email = item.get('Email'),
                            Web = item.get('Web'),
                            Schedule = item.get('Schedule'),
                            About = item.get('About'),
                            Image = item.get('Image'),
                            Reviews_number = item.get('Reviews_number'),
                            )
            with self.engine.connect() as connection:
                connection.execute(upd)
        except:
            print("### ERROR ###\n" % sys.exc_info()[0])
            raise
        return item