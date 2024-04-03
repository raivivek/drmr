#
# drmr: A tool for submitting pipeline scripts to distributed resource
# managers.
#
# Copyright 2023 Vivek Rai 
#
# Licensed under Version 3 of the GPL or any later version
#

import collections
import logging
import os
import subprocess
import textwrap

import drmr
import drmr.drm.base
import drmr.util


class LSF(drmr.drm.base.DistributedResourceManager):
    name = 'LSF'

    default_job_template = textwrap.dedent(
        """
        #!/bin/bash

        #### LSF preamble

        #BSUB -J {{job_name}}
        {% if nodes %}
        #BSUB -n {{processors|default(1)}}
        {% endif %}
        {% if node_properties %}
        #BSUB -R "{{node_properties.split(',')[0]}}"
        {% endif %}
        #BSUB -R "rusage[mem={{memory|default('4000')}}]"
        #BSUB -M {{memory|default('4000')}}
        {% if time_limit %}
        #BSUB -W {{time_limit}}
        {% endif %}
        #BSUB -o "{{control_directory}}/{{job_name}}_%J.out"
        {% if account %}
        #BSUB -P {{account}}
        {% endif %}
        {% if destination %}
        #BSUB -q {{destination}}
        {% endif %}
        {% if email %}
        #BSUB -u {{email}}
        {% endif %}
        {% if mail_event_string %}
        #BSUB -B -N -u {{email}}
        {% endif %}
        {% if dependencies %}
        #BSUB -w "{{resource_manager.make_dependency_string(dependencies)}}"
        {% endif %}
        {% if working_directory %}
        #BSUB -cwd "{{working_directory}}"
        {% endif %}
        {% if array_controls %}
        #BSUB -J "{{job_name}}[{{array_controls['array_index_min']|default(1)}}-{{array_controls['array_index_max']|default(1)}}%{{array_controls['array_concurrent_jobs']}}]"
        {% endif %}

        #### End LSF preamble

        {% if notes %}
        #### Notes
        {{notes}}

        {% endif %}
        {% if environment_setup %}
        #### Environment setup
        {% for line in environment_setup %}
        {{line}}
        {% endfor %}

        {% endif %}

        ####  Commands

        {{command}}
        """
    ).lstrip()

    default_array_command_template = textwrap.dedent(
        """
        if [ "$LSB_JOBINDEX" -eq {{index}} ]; then
            {{command}}
        fi
        """
    )

    job_dependency_states = [
        'done',
        'not_started',
        'running',
    ]

    job_state_map = {
        'done': 'done',
        'not_started': 'not_started',
        'running': 'running',
        'start': '',
    }

    mail_event_map = {
        'BEGIN': 'begin',
        'END': 'end',
        'FAIL': 'fail',
    }

    def delete_jobs(self, job_ids=None, job_name=None, job_owner=None, dry_run=False):
        logger = self.get_method_logger()

        if job_ids is None:
            job_ids = []

        targets = set(job_ids)
        targets.update(self.get_active_job_ids(job_ids, job_name, job_owner))

        if targets:
            if dry_run:
                logger.info(self.explain_job_deletion(targets, job_name, job_owner, dry_run))
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(self.explain_job_deletion(targets, job_name, job_owner, dry_run))
                command = ['bkill'] + list(targets)
                try:
                    subprocess.check_call(command)
                except subprocess.CalledProcessError as e:
                    raise drmr.exceptions.DeletionError(e.returncode, e.cmd, e.output, targets)

    def get_active_job_ids(self, job_ids=None, job_name=None, job_owner=None):
        logger = self.get_method_logger()

        if job_ids is None:
            job_ids = []

        jobs = set([])

        command = [
            'bjobs',
            '-o "jobid job_name user stat"',
            '-noheader'
        ]

        bjobs_lines = self.capture_process_output(command).splitlines()
        for job in bjobs_lines:
            job_id, name, owner, state = job.split()
            if job_owner and owner != job_owner:
                continue

            if job_name and job_name not in name:
                continue

            if job_ids and job_id not in job_ids:
                continue

            jobs.add(job_id)

        if jobs:
            logger.debug('Found {} active jobs'.format(len(jobs)))
        else:
            logger.debug('No active jobs found.')

        return jobs

    def is_installed(self):
        output = ''
        try:
            output = self.capture_process_output(['bmod', '-V'])
        except:
            pass

        return 'LSF' in output

    def make_dependency_string(self, dependencies):
        dependency_string = ''
        if dependencies:
            dependency_list = []
            if not isinstance(dependencies, collections.Mapping):
                raise ValueError('Job data does not contain a map under the "dependencies" key.')
            for state, job_ids in list(dependencies.items()):
                if state not in self.job_dependency_states:
                    raise ValueError('Unsupported dependency state: %s' % state)

                # Translate dependency format from Slurm to LSF
                translated_state = state  # No direct mapping, so keep it the same
                translated_job_ids = ':'.join(str(job_id) for job_id in job_ids)
                dependency_list.append('done(%s:%s)' % (translated_state, translated_job_ids))
            dependency_string = ' && '.join(dependency_list)  # Use '&&' in LSF for AND dependencies

        return dependency_string


    def write_cancel_script(self, job_data, job_ids):
        logger = self.get_method_logger()
        logger.debug('Writing canceller script for {}'.format(job_data))

        self.set_control_directory(job_data)
        filename = drmr.util.absjoin(job_data['control_directory'], job_data['job_name'])
        if not os.path.exists(job_data['control_directory']):
            os.makedirs(job_data['control_directory'])
        with open(filename, 'w') as canceller:
            canceller.write('#!/bin/sh\n\n')
            for job in job_ids:
                canceller.write('bkill %s\n' % job)  # Use 'bkill' instead of 'scancel'
            os.chmod(filename, 0o755)


    def set_mail_event_string(self, job_data):
        if job_data.get('mail_events'):
            job_data['mail_event_string'] = ','.join(
                sorted(
                    self.mail_event_map[event] for event in job_data['mail_events']
                )
            )


    def submit(self, job_filename, hold=False):
        if not self.is_installed():
            raise drmr.exceptions.ConfigurationError('{} is not installed or not usable.'.format(self.name))

        try:
            command = ['bsub']
            if hold:
                command.append('-H')
            command.extend(['<', job_filename])

            job_id_line = self.capture_process_output(command)
            job_id = job_id_line.split()[1]
            return job_id
        except subprocess.CalledProcessError as e:
            raise drmr.exceptions.SubmissionError(e.returncode, e.cmd, e.output)

    def validate_destination(self, destination):
        if not self.is_installed():
            raise drmr.exceptions.ConfigurationError('{} is not installed or not usable.'.format(self.name))

        valid = False
        try:
            command = ['bqueues', '-l', destination]
            status = self.capture_process_output(command)
            if destination in status:
                valid = True
        except:
            pass

        return valid