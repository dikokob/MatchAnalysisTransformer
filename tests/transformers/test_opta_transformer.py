"""
    Test OPTA transformer
"""
import os
import pandas as pd
import json
from transformers.SpectrumMatchAnalysisTransformer import SpectrumMatchAnalysisTransformer
from transformers.OPTATransformer import OPTATransformer
from scripts_from_fed.opta_files_manipulation_functions import opta_event_file_manipulation


class TestOptaTransformer:
    """Test the opta transformer
    """
    path_match_results = 'tests/fixtures/srml-8-2019-f1059702-matchresults.xml'
    metadata_path='tests/fixtures/g1059702_SecondSpectrum_Metadata.json'

    path_events = 'tests/fixtures/crosses_loop_in_tracking/inputs/f24-8-2019-1059702-eventdetails.xml'
    spectrumTransformer = SpectrumMatchAnalysisTransformer('SpectrumMatchAnalysis')

    @staticmethod
    def get_opta_transformers():

        opta_transformer = OPTATransformer()
        return opta_transformer

    def test_get_shots_output_is_dataframe(self):
        """test that output is a pd.Dataframe
        """

        opta_transformer = self.get_opta_transformers()

        opta_event_data_df, opta_match_info = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        df_player_names_raw, _, _ = self.spectrumTransformer.get_player_match_data(
            match_results_xml=self.path_match_results,
            path_track_meta=self.metadata_path
        )
        output = opta_transformer.opta_shots(
            df_opta_events=opta_event_data_df,
            df_player_names_raw=df_player_names_raw,
            opta_match_info=opta_match_info
        )

        assert (type(output) is pd.DataFrame),(
            "Failed: Output was not of type pd.dataframe"
        )

    def test_get_shots_output_contains_required_fields(self):
        """test that output contains required fields
        """
        validation_fields = [
            'from_penalty',
            'on_target',
            'off_target'
        ]
        opta_transformer = self.get_opta_transformers()
        opta_event_data_df, opta_match_info = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        df_player_names_raw, _, _ = self.spectrumTransformer.get_player_match_data(
            match_results_xml=self.path_match_results,
            path_track_meta=self.metadata_path
        )
        output = opta_transformer.opta_shots(
            df_opta_events=opta_event_data_df,
            df_player_names_raw=df_player_names_raw,
            opta_match_info=opta_match_info
        )

        for field in validation_fields:
            assert (field in output.columns),(
                "Failed: Output did not contain = {}".format(field)
            )

    def test_get_shots_output_columns_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/crosses_loop_in_tracking/outputs/df_opta_shots.csv", index_col=None)
        opta_transformer = self.get_opta_transformers()
        opta_event_data_df, opta_match_info = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        df_player_names_raw, _, _ = self.spectrumTransformer.get_player_match_data(
            match_results_xml=self.path_match_results,
            path_track_meta=self.metadata_path
        )
        output = opta_transformer.opta_shots(
            df_opta_events=opta_event_data_df,
            df_player_names_raw=df_player_names_raw,
            opta_match_info=opta_match_info
        )

        verification_columns = verification_result.columns
        output_columns = output.columns

        overlap_check = ["missingInOutput:"+c for c in verification_columns if c not in output_columns] + ["missingInVerification:"+c for c in output_columns if c not in verification_columns]
        print(overlap_check)
        assert len(overlap_check) == 0, (
            "Failed: columns did not match up. Missing columns = {}"
            .format(overlap_check)
        )

    def test_get_shots_output_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/crosses_loop_in_tracking/outputs/df_opta_shots.csv", index_col=None)

        opta_transformer = self.get_opta_transformers()
        opta_event_data_df, opta_match_info = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        df_player_names_raw, _, _ = self.spectrumTransformer.get_player_match_data(
            match_results_xml=self.path_match_results,
            path_track_meta=self.metadata_path
        )
        output = opta_transformer.opta_shots(
            df_opta_events=opta_event_data_df,
            df_player_names_raw=df_player_names_raw,
            opta_match_info=opta_match_info
        )

        print(verification_result, output)
        # (verification_result.values == output.values).all()
        assert (verification_result.values == output.values).all(), (
            "Failed: did not return required output dataframe"
        )

    def test_opta_cross_classification_output_is_dataframe(self):
        """test that output is a pd.Dataframe
        """
        opta_transformer = self.get_opta_transformers()
        opta_event_data_df, _ = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        output = opta_transformer.opta_crosses_classification(opta_event_data_df)

        assert (type(output) is pd.DataFrame),(
            "Failed: Output was not of type pd.dataframe"
        )

    def test_opta_cross_classification_output_contains_required_fields(self):
        """test that output contains required fields
        """
        validation_fields = [
            'competition_id', 'competition_name', 'season_id', 'season_name',
            'game_id', 'match_day', 'game_date', 'cross_to_remove',
            'out_of_pitch', 'ending_too_wide', 'OPTA Pull Back Qualifier'
        ]
        opta_transformer = self.get_opta_transformers()
        opta_event_data_df, _ = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        output = opta_transformer.opta_crosses_classification(opta_event_data_df)

        for field in validation_fields:
            assert (field in output.columns),(
                "Failed: Output did not contain = {}".format(field)
            )

    def test_opta_cross_classification_output_columns_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/crosses_loop_in_tracking/outputs/df_crosses_classification.csv", index_col=None)

        opta_transformer = self.get_opta_transformers()
        opta_event_data_df, _ = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        output = opta_transformer.opta_crosses_classification(opta_event_data_df)

        assert all(verification_result.columns == output.columns), (
            "Failed: did not return required output dataframe"
        )

    def test_opta_cross_classification_output_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/crosses_loop_in_tracking/outputs/df_crosses_classification.csv", index_col=None)

        opta_transformer = self.get_opta_transformers()
        opta_event_data_df, _ = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        output = opta_transformer.opta_crosses_classification(opta_event_data_df)

        assert (verification_result.values == output.values).all(), (
            "Failed: did not return required output dataframe"
        )

    def test_opta_only_open_play_crosses_output_is_dataframe(self):
        """test that output is a pd.Dataframe
        """
        data, _, _, _, _, _, _, _, _ = opta_event_file_manipulation(self.path_events)

        output = data.groupby(['unique_event_id']).apply(OPTATransformer.only_open_play_crosses)
        assert (type(output) is pd.DataFrame),(
            "Failed: Output was not of type pd.dataframe"
        )

    def test_opta_only_open_play_crosses_output_contains_required_fields(self):
        """test that output contains required fields
        """
        validation_fields = [
            'competition_id', 'competition_name', 'season_id', 'season_name',
            'game_id', 'match_day', 'game_date', 'period_1_start', 'period_2_start',
            'home_score', 'home_team_id', 'home_team_name', 'away_score',
            'away_team_id', 'away_team_name', 'fixture', 'result',
            'unique_event_id', 'event_id', 'period_id', 'min', 'sec', 'timestamp',
            'type_id', 'player_id', 'team_id', 'outcome', 'keypass', 'assist', 'x',
            'y', 'qualifier_id', 'value', 'open_play_cross'
        ]
        data, _, _, _, _, _, _, _, _ = opta_event_file_manipulation(self.path_events)

        output = data.groupby(['unique_event_id']).apply(OPTATransformer.only_open_play_crosses)

        print(output)
        print(output.columns)
        for field in validation_fields:
            assert (field in output.columns),(
                "Failed: Output did not contain = {}".format(field)
            )

    def test_opta_only_open_play_crosses_output_columns_as_expected(self):
        """test output is as expected
        """
        verification_df = pd.read_csv("tests/fixtures/crosses_loop_in_tracking/outputs/df_only_open_play_crosses.csv", index_col=None)
        verification_df = verification_df.where(pd.notnull(verification_df), None)
        data, _, _, _, _, _, _, _, _ = opta_event_file_manipulation(self.path_events)

        output = data.groupby(['unique_event_id']).apply(OPTATransformer.only_open_play_crosses)
        output = output.where(pd.notnull(output), None)

        assert (verification_df.equals(output)), (
            "Failed: did not return required output dataframe"
        )

    def test_opta_only_open_play_crosses_output_as_expected(self):
        """test output is as expected
        """

        verification_df = pd.read_csv("tests/fixtures/crosses_loop_in_tracking/outputs/df_only_open_play_crosses.csv", index_col=None)
        verification_df = verification_df.where(pd.notnull(verification_df), None)
        data, _, _, _, _, _, _, _, _ = opta_event_file_manipulation(self.path_events)

        output = data.groupby(['unique_event_id']).apply(OPTATransformer.only_open_play_crosses)
        output = output.where(pd.notnull(output), None)

        assert (verification_df.equals(output)), (
            "Failed: did not return required output dataframe"
        )
